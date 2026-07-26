package io.semanticdf.lineage

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

/** Extract base-column references from a Spark SQL expression.
  *
  * This is the **lineage** extractor — distinct from
  * [[io.semanticdf.CatalystColumnValidator]], which is the **validation**
  * extractor. The two share the same parsing path
  * ([[org.apache.spark.sql.catalyst.parser.CatalystSqlParser]] +
  * [[org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute]] walk)
  * but produce different shapes:
  *
  *   - **Validation** ([[io.semanticdf.CatalystColumnValidator]]):
  *     lowercase, unqualified (last name segment only). Fine for
  *     "is this column in the visible set?" — matches Spark's own
  *     case-insensitive column resolution.
  *
  *   - **Lineage** (this object): case-preserved, qualifier-preserved
  *     (the full `[[qualifier, ..., ]name]` list from the
  *     `UnresolvedAttribute`). Fine for "what did the user write
  *     in their `expr:` field?" — the consumer (human or LLM)
  *     wants to see `"Customers.OrderDate"`, not `"orderdate"`.
  *
  * Empty `Seq` for constant expressions (no column references).
  * Throws nothing — invalid SQL is treated as "we couldn't extract",
  * which the caller (the lineage builder) translates to
  * `LineageStatus.Partial`. */
private[semanticdf] object ColumnRefExtractor {

  /** Parse `expr` and return the column references in source order.
    *
    * Each reference is the `UnresolvedAttribute` nameParts joined by `.`,
    * preserving the user's original case. Duplicates are preserved
    * (the consumer can dedupe if it wants).
    *
    * @param expr the SQL expression (e.g. `"upper(carrier)"` or
    *             `"case when distance > 1000 then 'long' else 'short' end"`)
    * @return the column references, in the order they appear in the
    *         expression. Empty for constants. */
  def extract(expr: String): Seq[String] = {
    val ast = try CatalystSqlParser.parseExpression(expr)
    catch { case _: Exception => return Seq.empty }
    collectColumns(ast)
  }

  /** Walk the parsed AST and return every column reference in
    * source-order. Duplicates are preserved. */
  private def collectColumns(e: Expression): Seq[String] = {
    import scala.collection.mutable.ArrayBuffer
    val out = ArrayBuffer.empty[String]
    e.transform {
      case ua @ UnresolvedAttribute(nameParts) =>
        out += nameParts.mkString(".")
        ua
    }
    out.toSeq
  }
}
