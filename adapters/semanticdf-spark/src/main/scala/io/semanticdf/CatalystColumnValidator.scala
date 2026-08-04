package io.semanticdf

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

/** Internal template-method base for Spark SQL column-reference validators (R2).
  *
  * Consolidates the duplicate logic previously copy-pasted between
  * [[ExpressionValidator]] and [[SparkFilterValidator]]:
  *  - parsing via [[CatalystSqlParser]];
  *  - walking the AST for [[UnresolvedAttribute]] references;
  *  - normalizing both parsed refs and visible source columns to lower case
  *    so a filter `Origin IS NOT NULL` against a column `Origin` is accepted
  *    (matches Spark's default case-insensitive column resolution);
  *  - the short-circuit "no column references → expression is constant".
  *
  * The two existing validators differ only in:
  *  - their error-message wording (which columns are visible, which block
  *    the filter is for);
  *  - which "context" is passed to the protected hooks
  *    [[parseFailure]] and [[missingColumns]].
  *
  * Both subclasses keep their existing public methods and signatures.
  * Tests under `VersionAndValidatorSpec` and `RowFilterWalkSpec` cover
  * the existing behaviors and must continue to pass unchanged.
  *
  * Internal-only — `private[semanticdf]` so external code can't depend
  * on it. The visible behavior (YAML load-time error reporting) is
  * unchanged.
  */
private[semanticdf] abstract class CatalystColumnValidator[C] {

  /** Build the parse-failure exception. Each subclass renders its
    * own message (e.g. "Filter 'X' on model 'Y': failed to parse..."). */
  protected def parseFailure(
      context: C,
      expression: String,
      cause: Exception,
  ): IllegalArgumentException

  /** Build the missing-column exception. Subclasses include the relevant
    * context in the message (visible columns set, block kind, etc.). */
  protected def missingColumns(
      context: C,
      missing: Set[String],
      visible: Set[String],
      parsed: String,
  ): IllegalArgumentException

  /** Parse `expression` via the shared Catalyst parser and check that every
    * column reference it mentions is present in `visible` (case-insensitive).
    * Throws via the subclass's [[parseFailure]] / [[missingColumns]] on
    * failure. Returns silently on success, including on constant
    * expressions that reference no columns. */
  protected final def validateColumns(
      expression: String,
      visible: Set[String],
      context: C,
  ): Unit = {
    val ast = try CatalystSqlParser.parseExpression(expression)
    catch { case e: Exception => throw parseFailure(context, expression, e) }

    val refs = collectColumns(ast)
    if (refs.isEmpty) return  // constant expression: no columns to check

    val visibleLower = visible.iterator.map(_.toLowerCase).toSet
    val missing = refs.filterNot(visibleLower.contains)
    if (missing.nonEmpty) throw missingColumns(context, missing, visible, expression)
  }

  /** Walk the parsed AST and return every column reference, lowercased and
    * unqualified (using the last name segment). UnresolvedAttribute can be
    * qualified (`table.col`) or unqualified (`col`); we keep the last segment
    * to match against a source's `.columns` (which never carries table
    * qualifiers). */
  private def collectColumns(e: Expression): Set[String] =
    e.collect { case UnresolvedAttribute(nameParts) =>
      nameParts.last.toLowerCase
    }.toSet
}
