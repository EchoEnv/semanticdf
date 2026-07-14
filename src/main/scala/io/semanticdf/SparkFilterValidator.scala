package io.semanticdf

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser


/** Validates a Spark SQL filter expression intended for [[SemanticRowFilterOp]].
  *
  * The rule: the expression's column references must all be visible in the
  * source DataFrame's columns. Joins haven't run yet at filter time — columns
  * from joined models are NOT visible to a row filter.
  *
  * Used by the YamlLoader to fail fast (at model-load time) instead of letting
  * a misconfigured filter explode deep inside a query.
  *
  * Implementation: parses the expression via Spark's [[CatalystSqlParser]] and
  * walks the AST collecting [[UnresolvedAttribute]] nodes (column references).
  * Both the parser and the AST are public Spark APIs.
  */
private[semanticdf] object SparkFilterValidator {

  /** Throws `IllegalArgumentException` on parse failure or column-availability failure.
    *
    * @param expr          the Spark SQL filter expression from the YAML `expr:` field
    * @param sourceColumns the columns of THIS model's source DataFrame
    * @param modelName     the model name (for error messages)
    * @param filterName    the filter entry's name (for error messages)
    */
  def validate(
      expr: String,
      sourceColumns: Set[String],
      modelName: String,
      filterName: String,
  ): Unit = {
    val ast = try {
      CatalystSqlParser.parseExpression(expr)
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(
          s"Filter '$filterName' on model '$modelName': failed to parse Spark SQL " +
          s"expression '$expr': ${e.getMessage}",
          e)
    }

    val cols = collectColumns(ast)
    if (cols.isEmpty) {
      // Predicate has no column reference (e.g., `1 = 1`). Permit but note.
      return
    }
    val missing = cols.filterNot(sourceColumns.contains)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(
        s"Filter '$filterName' on model '$modelName' references column(s) " +
        s"${missing.toSeq.sorted.map("\"" + _ + "\"").mkString(", ")} that are not " +
        s"present on this model's source table. Filters are pre-join: only the " +
        s"model's own source columns are visible at filter time. Apply cross-table " +
        s"predicates via query-time .where(...) instead.")
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
