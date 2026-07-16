package io.semanticdf

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser


/** Validates that a Spark SQL expression references only columns that are
  * visible at the point of evaluation.
  *
  * Sibling of [[SparkFilterValidator]] (which is purpose-built for the
  * `filters:` block — boolean predicates, pre-join semantics). This validator
  * is more general: it's used for `dimensions:`, `transforms:`, and `measures:`
  * expressions, which can be any Spark SQL expression (arithmetic, `case when`,
  * `datediff`, window functions, etc.).
  *
  * ==Why==
  *
  * Without this, a typo in a dimension's SQL expression (e.g.
  * `case when carrrier in (...) then ...` instead of `carrier`) loads silently
  * and surfaces only when the dimension is first materialized in a query, as a
  * cryptic Spark `UNRESOLVED_COLUMN.WITH_SUGGESTION` error. We catch it at
  * model-load time instead.
  *
  * ==Implementation==
  *
  * Parses the expression via Spark's [[CatalystSqlParser]] (the same parser
  * Spark itself uses — handles the full SQL grammar) and walks the AST
  * collecting [[UnresolvedAttribute]] nodes (column references). Column refs
  * are case-insensitive (matching Spark's default resolution), so both sides
  * are lowercased before comparison.
  *
  * ===Visibility rules===
  *
  * The caller passes the set of columns visible at the point the expression
  * is evaluated:
  *
  *   - For dimensions: the source DataFrame's columns (transforms haven't run yet
  *     at model-load time, so transform outputs aren't visible to dims either).
  *   - For transforms: source columns PLUS outputs of any previously-declared
  *     transforms (declaration order in YAML matters).
  *   - For base measures: source columns PLUS outputs of any transforms
  *     (measures run after transforms).
  *   - For calculated measures: not used — CalcExpr references other measures
  *     by name, not columns; the resolution happens at query time.
  */
private[semanticdf] object ExpressionValidator {

  /** Throws `IllegalArgumentException` if the expression references any
    * column not in `visibleColumns`.
    *
    * @param expr            the Spark SQL expression from the YAML `expr:` field
    * @param visibleColumns  columns visible at this point (source + earlier transforms)
    * @param kind            short label for the YAML block: "dimension", "transform", "measure"
    * @param modelName       the model name (for error messages)
    * @param fieldName       the entry's name within the block (for error messages)
    */
  def validate(
      expr: String,
      visibleColumns: Set[String],
      kind: String,
      modelName: String,
      fieldName: String,
  ): Unit = {
    val ast = try {
      CatalystSqlParser.parseExpression(expr)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"$kind '$fieldName' on model '$modelName': failed to parse Spark SQL " +
          s"expression '$expr': ${e.getMessage}",
          e)
    }

    val cols = collectColumns(ast)
    if (cols.isEmpty) {
      // Constant expression (e.g., `1`, `'foo'`, `now()`). No column refs to check.
      return
    }

    val missing = cols.filterNot(visibleColumns.map(_.toLowerCase).contains)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"$kind '$fieldName' on model '$modelName' references column(s) " +
        s"${missing.toSeq.sorted.map("\"" + _ + "\"").mkString(", ")} that are not " +
        s"visible at this point. Visible columns: " +
        s"${visibleColumns.toSeq.sorted.map("\"" + _ + "\"").mkString(", ")}. " +
        s"Did you misspell a column name? For derived columns, declare them in " +
        s"`transforms:` first (transforms run before measures and are visible to them)."
      )
    }
  }

  /** Walks the parsed AST and returns all column references (unqualified, lowercased). */
  private def collectColumns(e: Expression): Set[String] =
    e.collect { case UnresolvedAttribute(nameParts) =>
      // UnresolvedAttribute can be qualified (`table.col`) or unqualified (`col`).
      // We compare against `df.columns` which never carries table qualifiers,
      // so use the LAST segment as the column name. Lower-case for case-insensitive match.
      nameParts.last.toLowerCase
    }.toSet
}