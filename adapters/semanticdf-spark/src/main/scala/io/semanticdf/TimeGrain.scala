package io.semanticdf

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.date_trunc

/** Time-grain truncation for time dimensions ().
  *
  * A time grain truncates a timestamp to a fixed unit (year/quarter/month/week/day/
  * hour/minute/second) so rows can be grouped at that granularity. Spark's
  * [[org.apache.spark.sql.functions.date_trunc date_trunc(unit, col)]] does the
  * truncation natively.
  *
  * Grains are ordered finest→coarsest for validation against a dimension's
  * `smallestTimeGrain`: a request finer than the allowed minimum raises (the classic
  * "data only has daily resolution, can't group by hour" trap).
  *
  * Mirrors BSL's `TIME_GRAIN_ORDER` / `TIME_GRAIN_TRANSFORMATIONS` / `_validate_time_grain`.
  * Both short (`"month"`) and canonical (`"TIME_GRAIN_MONTH"`) names are accepted.
  */
object TimeGrain {

  /** Grain identifier (canonical Spark `date_trunc` unit). */
  type Grain = String

  /** Grains finest→coarsest; indices used for fineness comparison. */
  val Order: Seq[Grain] =
    Seq("SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR")

  private val shortToCanonical: Map[String, Grain] =
    Order.map(g => g -> g).toMap ++ Order.map(g => s"TIME_GRAIN_$g" -> g).toMap ++
      Order.map(g => g.toLowerCase -> g).toMap

  /** Normalize a grain name to its canonical Spark unit.
    *
    * Accepts `"month"`, `"MONTH"`, `"TIME_GRAIN_MONTH"`, `"TIME_GRAIN_month"`.
    * Throws on unknown grains. */
  def normalize(grain: String): Grain =
    shortToCanonical.get(grain) match {
      case Some(unit) => unit
      case None =>
        // Try uppercasing the bare form (e.g. "month" already covered, but "Month" isn't).
        shortToCanonical.get(grain.toUpperCase) match {
          case Some(unit) => unit
          case None =>
            throw new IllegalArgumentException(
              s"Invalid time grain '$grain'. Valid: ${Order.map(g => g.toLowerCase).mkString(", ")} " +
                s"(or TIME_GRAIN_<NAME>).")
        }
    }

  /** Validate that `requested` is not finer than `smallestAllowed` for `dimName`.
    *
    * Both are normalized first. `smallestAllowed = None` skips validation. Raises
    * `IllegalArgumentException` when the requested grain's index in [[Order]] is less
    * than the allowed grain's index (i.e. finer). */
  def validateNotFiner(requested: String, smallestAllowed: Option[String], dimName: String): Unit =
    smallestAllowed.foreach { raw =>
      val allowed = normalize(stripGrainPrefix(raw))
      val req     = normalize(requested)
      val reqIdx  = Order.indexOf(req)
      val minIdx  = Order.indexOf(allowed)
      if (reqIdx >= 0 && minIdx >= 0 && reqIdx < minIdx)
        throw new IllegalArgumentException(
          s"Requested time grain '${req.toLowerCase}' is finer than the smallest allowed " +
            s"grain '${allowed.toLowerCase}' for time dimension '$dimName'.")
    }

  /** Strip a leading `TIME_GRAIN_` from a stored smallest-grain value, if present. */
  private def stripGrainPrefix(s: String): String =
    if (s.startsWith("TIME_GRAIN_")) s.substring("TIME_GRAIN_".length) else s

  /** Truncate a column to `grain`. `grain` should already be [[normalize normalized]]. */
  def truncate(grain: Grain, col: Column): Column = date_trunc(grain, col)

  /** True if `g1` is at-least-as-fine as `g2` (i.e., g1's index in
    * [[Order]] is <= g2's). Both must be normalized first. Throws on
    * unknown grain (parse don't validate). */
  def finerOrEqual(g1: Grain, g2: Grain): Boolean =
    orderIndex(g1) <= orderIndex(g2)

  /** True if `g1` is strictly finer than `g2`. */
  def finer(g1: Grain, g2: Grain): Boolean =
    orderIndex(g1) < orderIndex(g2)

  /** Index of `g` in [[Order]]; throws on unknown. */
  private def orderIndex(g: Grain): Int =
    Order.indexOf(g) match {
      case -1 => throw new IllegalArgumentException(
        s"Unknown time grain '$g'. Valid: ${Order.map(_.toLowerCase).mkString(", ")}")
      case idx => idx
    }
}
