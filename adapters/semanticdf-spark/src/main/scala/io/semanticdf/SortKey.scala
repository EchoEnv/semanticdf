package io.semanticdf

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/** Sort-key DSL for [[SemanticTable.orderBy]] / [[SemanticTable.query]].
  *
  * A bare string is ascending; wrap in [[SortKey.desc]] for descending:
  * {{{
  * st.orderBy("carrier", SortKey.desc("total_passengers"))
  * }}}
  */
sealed trait SortKey {
  private[semanticdf] def toColumn: Column
}
object SortKey {

  /** Wrap a column name in backticks if it contains characters that
    * Spark's `col(...)` would misinterpret — notably `.` (treated as a
    * table/struct qualifier). Joined dimensions are named `alias.column`
    * (e.g. `customers.signup_date`); without quoting, `col("customers.x")`
    * looks for a nested struct field instead of the literal column.
    * Names already wrapped in backticks (by the caller) are left as-is,
    * so this is backward-compatible with manual `` SortKey.asc(s"`x`") ``.
    * Simple identifiers are returned unchanged. */
  private def quote(name: String): String =
    if (name.startsWith("`")) name
    else if (name.matches("[a-zA-Z_][a-zA-Z0-9_]*")) name
    else s"`$name`"

  private[semanticdf] final case class Asc(name: String)  extends SortKey { def toColumn = col(quote(name)).asc }
  private[semanticdf] final case class Desc(name: String) extends SortKey { def toColumn = col(quote(name)).desc }

  /** Explicit ascending key. */
  def asc(name: String): SortKey = Asc(name)
  /** Explicit descending key. */
  def desc(name: String): SortKey = Desc(name)

  /** Typed ascending key — reads the column name directly from the
    * [[SemanticField]] witness. Works for any field (dimension or measure),
    * so `SortKey.asc(carrier)`, `SortKey.desc(pax)` are both valid.
    *
    * The parameter is the typeclass instance itself (not a `FieldRef`), so
    * `SemanticDimension[F]` / `SemanticMeasure[F]` match by subtyping in
    * Scala's phase-1 overload resolution — no implicit conversion is needed,
    * and this overload is picked over `asc(name: String)` even from
    * cross-package consumer code. */
  def asc(field: SemanticField[_]): SortKey = Asc(field.name)

  /** Typed descending key — see [[asc(field)*]]. */
  def desc(field: SemanticField[_]): SortKey = Desc(field.name)

  /** Read the column-name field of any SortKey (private to avoid exposing the sealed
    * cases to public API). Used by [[SemanticTable.explainSemantic]]. */
  private[semanticdf] def nameOf(k: SortKey): String = k match {
    case Asc(n)  => n
    case Desc(n) => n
    case _       => ""
  }

  /** Implicit `String => SortKey` so `orderBy("carrier", SortKey.desc("x"))` works. */
  implicit def strToSortKey(name: String): SortKey = Asc(name)
}
