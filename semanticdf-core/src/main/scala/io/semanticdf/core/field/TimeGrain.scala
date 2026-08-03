package io.semanticdf.core.field

/** Engine-portable time-grain ADT — Phase 1 consolidation mirror.
  *
  * Mirrors `io.semanticdf.TimeGrain` with ONLY the data parts:
  *   - `Grain` type alias (String)
  *   - `Order` (finest→coarsest list of canonical grain names)
  *   - `normalize` (short / canonical / lowercase form → canonical)
  *   - `validate` (check that requested grain is not finer than allowed)
  *
  * The Spark-bearing `date_trunc` behavior stays in the original.
  * Engine-portable consumers (e.g. wire-format encoders, model
  * validators that don't need to compile a Spark Column) can use
  * this mirror without pulling in Spark.
  *
  * ==Why this exists==
  *
  * Time-grain truncation (year/quarter/month/week/day/hour/minute/second)
  * is a universal concept across query engines. The grain *order* and
  * *name normalization* rules are engine-portable. Only the actual
  * truncation (Spark's `date_trunc`, Trino's `date_trunc`, etc.) is
  * engine-specific.
  *
  * Future engine adapters use the core mirror for grain ordering and
  * validation, then emit their engine-specific SQL for truncation.
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports. Pure
  * data + pure functions (no I/O, no closures, no engine types).
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `Order` is a `Seq[String]`; `Grain` is `String`
  * - Pure functions: `normalize` and `validate` are referentially
  *   transparent (same input → same output, no side effects)
  * - Sealed-grain enumeration: `Order` is the canonical list, `Grain`
  *   type alias encodes the constraint
  */
object TimeGrain {

  /** Grain identifier (canonical Spark `date_trunc` unit name).
    *
    * Mirrors BSL's `TIME_GRAIN_ORDER` vocabulary. The canonical names
    * are uppercase (SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER,
    * YEAR); the engine-portable normalization accepts short, canonical,
    * and lowercase forms. */
  type Grain = String

  /** Grains finest→coarsest; indices used for fineness comparison.
    *
    * Order matters: a "finer" grain has a lower index in this Seq.
    * Validation against a dimension's `smallestTimeGrain` checks that
    * the requested grain's index is NOT less than the allowed grain's
    * index (i.e. NOT finer). */
  val Order: Seq[Grain] =
    Seq("SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR")

  /** Normalize a grain name to its canonical Spark unit.
    *
    * Accepts `"month"`, `"MONTH"`, `"TIME_GRAIN_MONTH"`, `"TIME_GRAIN_month"`.
    * Throws on unknown grains.
    *
    * Engine-portable: pure function over String → String, no Spark
    * types involved. */
  def normalize(grain: String): Grain = {
    val shortToCanonical: Map[String, Grain] =
      Order.map(g => g -> g).toMap ++
        Order.map(g => s"TIME_GRAIN_$g" -> g).toMap ++
        Order.map(g => g.toLowerCase -> g).toMap

    shortToCanonical.get(grain) match {
      case Some(unit) => unit
      case None =>
        // Try uppercasing the bare form (e.g. "Month" → "MONTH").
        shortToCanonical.get(grain.toUpperCase) match {
          case Some(unit) => unit
          case None =>
            throw new IllegalArgumentException(
              s"Invalid time grain '$grain'. Valid: ${Order.map(g => g.toLowerCase).mkString(", ")} " +
                s"(or TIME_GRAIN_<NAME>).")
        }
    }
  }

  /** Look up the index of a (normalized) grain in [[Order]].
    * Returns -1 if the grain is not in the canonical order list. */
  private[core] def indexOf(grain: Grain): Int = Order.indexOf(grain)

  /** Check if `requested` is not finer than `smallestAllowed`.
    *
    * Both inputs are normalized first via [[normalize]]. Returns true
    * when the requested grain is the same as or coarser than the
    * allowed grain (i.e. the request is acceptable). Returns false
    * when the requested grain is finer than allowed.
    *
    * Engine-portable: pure function over String → Boolean. */
  def isCoarserOrEqual(requested: String, smallestAllowed: String): Boolean = {
    val reqIdx = indexOf(normalize(requested))
    val allowIdx = indexOf(normalize(smallestAllowed))
    if (reqIdx == -1 || allowIdx == -1) {
      throw new IllegalArgumentException(
        s"Unknown grain: requested='$requested' allowed='$smallestAllowed'. " +
          s"Valid: ${Order.mkString(", ")}")
    }
    reqIdx >= allowIdx
  }
}