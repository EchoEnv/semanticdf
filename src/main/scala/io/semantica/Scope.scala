package io.semantica

import org.apache.spark.sql.{Column, DataFrame}

/** The `t` passed to dimension/measure lambdas (DESIGN §4.2).
  *
  * `apply(name)` resolves a field to a Spark [[Column]]. Resolution rules are the
  * public contract users program against (aligned with BSL's `IbisCalcScope`):
  * base column wins on collision, else known measure, else a typed error. Concrete
  * scopes differ only in *where* columns come from.
  *
  * `all(name)` (percent-of-total) is declared here but is **not implemented until
  * Phase 3** — it throws to make premature use loud.
  */
trait SemanticScope {

  /** Resolve `name` to a Column. */
  def apply(name: String): Column

  /** Percent-of-total reference (Phase 3). Not yet implemented. */
  def all(name: String): Column =
    throw new UnsupportedOperationException(
      "t.all(...) is not implemented until Phase 3 (percent-of-total)."
    )
}

object SemanticScope {

  /** Thrown when a lambda references a name that is neither a base column nor a known measure. */
  final class UnknownFieldError(name: String, candidates: Iterable[String])
      extends IllegalArgumentException(renderUnknownField(name, candidates))

  private def renderUnknownField(name: String, candidates: Iterable[String]): String = {
    val suggestion = closestMatch(name, candidates) match {
      case Some(c) => s" Did you mean: '$c'?"
      case None    => ""
    }
    s"'$name' is not a known column or measure.$suggestion"
  }

  private def closestMatch(name: String, candidates: Iterable[String]): Option[String] =
    if (name.isEmpty || candidates.isEmpty) None
    else candidates.minByOption(c => editDistance(name.toLowerCase, c.toLowerCase))

  private def editDistance(a: String, b: String): Int = {
    val n = a.length
    val m = b.length
    if (n == 0) return m
    if (m == 0) return n
    val prev = Array.tabulate(m + 1)(identity)
    val curr = new Array[Int](m + 1)
    var i = 1
    while (i <= n) {
      curr(0) = i
      var j = 1
      while (j <= m) {
        val cost = if (a(i - 1) == b(j - 1)) 0 else 1
        curr(j) = math.min(math.min(curr(j - 1) + 1, prev(j) + 1), prev(j - 1) + cost)
        j += 1
      }
      i += 1
      prev.indices.foreach(k => prev(k) = curr(k))
    }
    prev(m)
  }
}

/** Scope over a base DataFrame. `apply(name)` returns the base column when present.
  *
  * Used for plain base measures and for dimension expressions.
  */
final class BaseScope(df: DataFrame) extends SemanticScope {
  override def apply(name: String): Column =
    if (df.columns.contains(name)) df(name)
    else throw new SemanticScope.UnknownFieldError(name, df.columns)
}

/** Scope over an *aggregated* DataFrame (Phase 1b — calc-measure compilation).
  *
  * Columns on an aggregated DataFrame are the group-by keys plus the measure names.
  * `apply(name)` resolves by name against that DataFrame. This is the load-bearing
  * simplification of the port (DESIGN §6.1): a calc lambda is re-run against the real
  * aggregated DataFrame, and name identity is sufficient — no op-graph substitution.
  *
  * `knownMeasures` widens the suggestion set in the "did you mean?" error so a typo in a
  * measure name surfaces a useful hint. During *classification* (not execution), a known
  * measure that is not yet a real column resolves to a placeholder `lit(0.0)` so the
  * lambda typechecks; classification never executes the result.
  *
  * `totalsResolver` (Phase 3) maps a measure name to its grand-total column — the same
  * measure aggregated with no group keys, cross-joined into this DataFrame under a
  * `__total__` prefix. When present, `all(name)` resolves to it (percent-of-total).
  */
final class MeasureScope(
    df: DataFrame,
    knownMeasures: Set[String],
    totalsResolver: Option[String => Column] = None,
) extends SemanticScope {
  private val valid: Set[String] = df.columns.toSet ++ knownMeasures

  override def apply(name: String): Column =
    if (df.columns.contains(name)) df(name)
    else if (knownMeasures.contains(name)) org.apache.spark.sql.functions.lit(0.0)
    else throw new SemanticScope.UnknownFieldError(name, valid)

  /** Percent-of-total reference (Phase 3). Resolves to the grand-total column when a
    * `totalsResolver` was supplied by [[SemanticAggregateOp]] (i.e. the aggregation
    * detected `t.all(...)` usage and cross-joined a totals table). Throws otherwise. */
  override def all(name: String): Column = totalsResolver match {
    case Some(resolve) => resolve(name)
    case None =>
      throw new UnsupportedOperationException(
        s"t.all('$name') is used in a calc, but this aggregation did not build a totals " +
          "table. This happens inside a grand-total aggregation or a totals sub-computation " +
          "where percent-of-total has no meaning."
      )
  }
}
