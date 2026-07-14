package io.semanticdf

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, when}

/** Convenience helpers for calc-measure lambdas (Phase A hardening).
  *
  * Calc authors divide measures, and Spark's `/` operator returns `null` on divide-by-
  * zero (and on `x / null`). That is *correct SQL semantics*, but it silently propagates
  * `null` through a dashboard. [[safeDivide]] is an opt-in alternative that returns a
  * caller-chosen default instead.
  *
  * These are ordinary Spark `Column` expressions — semanticdf does not intercept division.
  * The default `/` is left untouched; `safeDivide` is for when silent nulls are
  * undesirable.
  */
object CalcHelpers {

  /** Divide `numerator` by `denominator`, returning `default` when the denominator is
    * null or zero.
    *
    * @example
    * {{{
    * Measure("pct", t => safeDivide(t("total"), t.all("total"), defaultValue = 0.0))
    * }}}
    *
    * Use this for percent-of-total and ratio calcs where a zero/missing denominator
    * (e.g. all rows filtered out) should yield a concrete `0.0` rather than `null`.
    * For ordinary division where `null` is the desired SQL result, keep using `/`.
    */
  def safeDivide(
      numerator: Column,
      denominator: Column,
      defaultValue: Any = null,
  ): Column =
    when(denominator.isNull || denominator === lit(0), lit(defaultValue))
      .otherwise(numerator / denominator)
}
