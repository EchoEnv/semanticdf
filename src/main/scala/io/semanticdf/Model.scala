package io.semanticdf

import org.apache.spark.sql.Column

/** Semantic model records (DESIGN §5.1).
  *
  * Both [[Dimension]] and [[Measure]] carry an expression `SemanticScope => Column`
  * plus metadata. They are immutable values users pass to
  * `SemanticTable#withDimensions(...)` / `withMeasures(...)`.
  */

/** A grouping field on a semantic model — the columns you `groupBy` on or filter
  * against. The "dimensions of a cube": `carrier`, `region`, `order_date`,
  * `customer_id`.
  *
  * Construct via the companion's factories:
  * {{{
  * val carrier    = Dimension("carrier",         t => t("carrier"))
  * val flightDate = Dimension.time("flight_date", t => t("flight_date"), smallestTimeGrain = Some("day"))
  * val customerId = Dimension.entity("customer_id", t => t("customer_id"))
  * }}}
  *
  * The `expr` function produces the Spark [[Column]] for this dimension at
  * compile time. `t` is a [[SemanticScope]] — call `t("col_name")` to reference
  * a source column, or compose Spark expressions on top of it
  * (`upper(t("carrier"))`, `t("ts").cast("date")`). The lambda is invoked
  * exactly twice per query — once to classify dependencies, once to produce
  * the Column — and never per row.
  *
  * `Dimension` is a `final class` (not a case class) so that fields could be
  * added later without breaking binary compatibility. Use [[copy]] to derive
  * a new dimension from an existing one.
  *
  * @param metadata optional key-value pairs for catalog tooling — e.g.
  *                `Map("owner" -> "analytics", "pii" -> "true")`. Doesn't
  *                affect query compilation; surfaced by `describe_model`
  *                and `okfgen`.
  * @param smallestTimeGrain (time dimensions only) the finest grain allowed.
  *                          A `query(timeGrain = "hour")` against a dimension
  *                          with `smallestTimeGrain = Some("day")` raises
  *                          `IllegalArgumentException`. `None` = no restriction.
  */
final class Dimension(
    val name: String,
    val expr: SemanticScope => Column,
    val description: Option[String] = None,
    val metadata: Map[String, String] = Map.empty,
    val isEntity: Boolean = false,
    val isTimeDimension: Boolean = false,
    val isEventTimestamp: Boolean = false,
    val smallestTimeGrain: Option[String] = None,
    /** Original expression string (e.g. the YAML `expr:` value or the
      * programmatic hint). Carried so consumers like `DescribeModel` can
      * surface a human-readable expression instead of the lambda's
      * opaque `toString`. `None` for dimensions built from a bare lambda
      * with no hint; the DescribeModel fallback then shows the lambda. */
    val exprString: Option[String] = None,
) extends Serializable {

  /** Returns a copy of this dimension with zero or more fields replaced.
    * Present because `Dimension` is a `final class` (not a case class), so
    * the standard case-class `copy` is unavailable.
    *
    * {{{
    * val withDesc = carrier.copy(description = Some("Airline carrier code"))
    * }}}
    *
    * Every parameter defaults to the current field value, so you only name
    * the ones you want to change. */
  def copy(
      name: String = this.name,
      expr: SemanticScope => Column = this.expr,
      description: Option[String] = this.description,
      metadata: Map[String, String] = this.metadata,
      isEntity: Boolean = this.isEntity,
      isTimeDimension: Boolean = this.isTimeDimension,
      isEventTimestamp: Boolean = this.isEventTimestamp,
      smallestTimeGrain: Option[String] = this.smallestTimeGrain,
      exprString: Option[String] = this.exprString,
  ): Dimension = new Dimension(name, expr, description, metadata, isEntity, isTimeDimension, isEventTimestamp, smallestTimeGrain, exprString)

  override def equals(that: Any): Boolean = that match {
    case d: Dimension =>
      name == d.name &&
      description == d.description &&
      metadata == d.metadata &&
      isEntity == d.isEntity &&
      isTimeDimension == d.isTimeDimension &&
      isEventTimestamp == d.isEventTimestamp &&
      smallestTimeGrain == d.smallestTimeGrain &&
      exprString == d.exprString &&
      expr.toString == d.expr.toString  // functions have no value equality; compare source
    case _ => false
  }
  override def hashCode(): Int = (name, expr, exprString, description, metadata, isEntity, isTimeDimension, isEventTimestamp, smallestTimeGrain).##
  override def toString: String = s"Dimension($name,${if (description.isDefined) description.get else "_"})$$"
}

/** Companion for the [[Dimension]] class. Provides three construction entry
  * points:
  *
  *   - [[apply]]   — a plain grouping field (`carrier`, `region`)
  *   - [[time]]    — a timestamp/date column for time-based grouping (`order_date`)
  *   - [[entity]]  — a primary-identifier column (`customer_id`, `session_id`)
  *
  * All three return an immutable `Dimension`; attach one to a model via
  * `SemanticTable#withDimensions(...)`. */
object Dimension {

  /** Construct a plain (non-time, non-entity) dimension.
    *
    * {{{
    * import io.semanticdf._
    *
    * val carrier = Dimension("carrier", t => t("carrier"))
    *
    * val region = Dimension(
    *   "region",
    *   t => t("country").substr(0, 2),   // any Spark Column expression
    *   description = Some("Region code (first 2 chars of country)"),
    *   metadata    = Map("owner" -> "geo-team"),
    * )
    * }}}
    *
    * @param name the dimension name; also the column alias in compiled output
    * @param expr a function `SemanticScope => Column`. The scope `t` lets you
    *             reference source columns (`t("carrier")`) or compose Spark
    *             expressions on top of them (`upper(t("carrier"))`). See
    *             [[SemanticScope]] for the full API.
    * @param description optional human-readable text; shown by `describe_model`
    *                     and emitted into OKF catalogs
    * @param metadata optional key-value pairs; don't affect query compilation
    */
  def apply(
      name: String,
      expr: SemanticScope => Column,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): Dimension = new Dimension(name, expr, description, metadata)

  /** Construct a time dimension — a timestamp or date column you want to group
    * or filter queries by time period.
    *
    * Once attached to a model, a time dimension unlocks:
    *   - truncation: `model.atTimeGrain(flightDate, "month")`
    *   - range filtering: `model.query(timeRange = "2024-01-01" -> "2024-03-31")`
    *   - auto-truncation: `model.query(timeGrain = "month")`
    *
    * {{{
    * import io.semanticdf._
    *
    * val flightDate = Dimension.time(
    *   "flight_date",
    *   t => t("flight_date"),
    *   smallestTimeGrain = Some("day"),  // queries finer than "day" will be rejected
    * )
    * }}}
    *
    * Requesting a finer grain than `smallestTimeGrain` (e.g. `query(timeGrain = "hour")`
    * against `Some("day")`) raises `IllegalArgumentException` at query time.
    *
    * @param smallestTimeGrain the finest grain allowed — one of `"year"`,
    *                          `"quarter"`, `"month"`, `"week"`, `"day"`,
    *                          `"hour"`, `"minute"`. `None` means no restriction.
    * @param isEventTimestamp `true` if this column is the table's event-time
    *                          column. Reserved for the future streaming terminal
    *                          Reserved for the future streaming terminal; no effect in batch today.
    */
  def time(
      name: String,
      expr: SemanticScope => Column,
      smallestTimeGrain: Option[String] = None,
      isEventTimestamp: Boolean = false,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): Dimension = new Dimension(
    name, expr, description, metadata,
    isEntity = false,
    isTimeDimension = true,
    isEventTimestamp = isEventTimestamp,
    smallestTimeGrain = smallestTimeGrain,
  )

  /** Construct an entity dimension — a column that identifies the distinct
    * subjects of a fact table (customers, sessions, users, orders).
    *
    * {{{
    * import io.semanticdf._
    *
    * val customerId = Dimension.entity("customer_id", t => t("customer_id"))
    * }}}
    *
    * Marking a dimension as an entity is a hint for catalog tooling
    * (`introspect`, `okfgen`) — it tells those tools to treat the column as
    * a primary identifier rather than an ordinary grouping field. No effect
    * on query compilation.
    */
  def entity(
      name: String,
      expr: SemanticScope => Column,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): Dimension = new Dimension(name, expr, description, metadata, isEntity = true)
}

/** A numeric aggregate on a semantic model — the values you compute across
  * groups. `flight_count`, `total_revenue`, `avg_distance`.
  *
  * {{{
  * import io.semanticdf._
  * import org.apache.spark.sql.functions._
  *
  * val flightCount  = Measure("flight_count",   t => count(lit(1)))
  * val totalRevenue = Measure("total_revenue",  t => sum(t("amount")))
  *
  * // calc measure: references other measures by name (compiled as a layer
  * // on top of the aggregated result, not in the agg() call itself)
  * val avgPerOrder  = Measure("avg_per_order",  t => t("total_revenue") / t("flight_count"))
  * }}}
  *
  * Each measure's `expr` is one of:
  *   - '''base''' — references source columns (`sum(t("distance"))`); compiled
  *     into the Spark `agg(...)` call.
  *   - '''calc''' — references other measure ''names'' (`t("total") / t("count")`);
  *     compiled as a `select` layer on top of the aggregated result.
  *
  * The framework classifies automatically by inspecting the lambda at compile
  * time — you never mark a measure as base or calc. Calc measures can also use
  * `t.all("name")` for percent-of-total (grand total across all groups); see
  * [[SemanticScope.all]].
  *
  * `Measure` is a `case class`; the synthetic `apply` / `copy` / `unapply`
  * methods are available as usual.
  *
  * @param metadata same role as [[Dimension.metadata]]; doesn't affect compilation.
  */
final case class Measure(
    name: String,
    expr: SemanticScope => Column,
    description: Option[String] = None,
    metadata: Map[String, String] = Map.empty,
    /** Original expression string (e.g. the YAML `expr:` value or the
      * programmatic hint). See [[Dimension.exprString]] for the full
      * rationale. `None` for measures built from a bare lambda with
      * no hint; the DescribeModel fallback then shows the lambda. */
    exprString: Option[String] = None,
)

/** Companion for [[Measure]] — extends the case class's synthesized
  * `apply` with a `typed[T]` factory. The typed form lets the user write
  * `Measure.typed[Double]("name", fn)` where the lambda's return type is
  * type-checked at compile time. See [[TypedMeasure]] and
  * [[TypedArithmetic]] for the typed-arithmetic DSL.
  *
  * The returned [[Measure]] is the untyped form, so existing call sites
  * (`.withMeasures(measures: Measure*)`, pattern matching on `Measure`,
  * etc.) work unchanged. The type parameter `T` is purely a compile-time
  * check on the lambda body; at runtime, T is erased and the lambda is
  * bridged via [[TypedMeasure.toMeasure]].
  *
  * Note on naming: the original plan called for `Measure[Double](name, fn)`
  * syntax. Implementing that as a second `apply[T]` overload on this
  * companion conflicts with the case class's synthesized `apply` when
  * a literal lambda is passed (Scala 2.13 can't disambiguate the lambda's
  * parameter type from the two overloads). The `typed` factory method
  * sidesteps the overload and keeps every existing call site working
  * unchanged. */
object Measure {
  /** Typed measure factory.
    *
    * {{{
    *   import io.semanticdf.TypedArithmetic.divide
    *
    *   Measure.typed[Double]("avg_passengers", t =>
    *     divide[Long, Long, Double](t("total_passengers"), t("flight_count")))
    * }}}
    *
    * The `t` parameter is a [[TypedSemanticScope]]; `t("col_name")`
    * returns a [[TypedColumn]] carrying the user-declared static type
    * of the column. Compose with [[TypedArithmetic]] functions for
    * compile-time type checks on `+`, `-`, `*`, `/`.
    *
    * The legacy untyped form `Measure("name", fn)` where `fn: SemanticScope
    * => Column` continues to work — the case class's synthesized
    * `apply` is the only `apply` on this companion. */
  def typed[T](
      name: String,
      expr: TypedSemanticScope => TypedColumn[T],
  ): Measure =
    TypedMeasure[T](name, expr).toMeasure
}

/** A per-row transformation applied to the source data at model-load time.
  *
  * Use a `Transform` for per-row logic that doesn't fit a measure's aggregate
  * context: `datediff(a, b)`, `case when ...`, `row_number() over (...)`. The
  * transform's output becomes a new column on the source DataFrame, visible to
  * subsequent transforms, dimensions, and measures.
  *
  * Transforms correspond to dbt's [https://docs.getdbt.com/docs/build/staging-models staging models]
  * and LookML's [https://cloud.google.com/looker/docs/derived-tables `derived_table`]
  * — the canonical place for per-row data prep.
  *
  * {{{
  * import io.semanticdf._
  * import org.apache.spark.sql.functions._
  *
  * val orders = toSemanticTable(ordersDf, name = Some("orders"))
  *   .withTransforms(
  *     Transform("ship_days",    t => datediff(t("shipped_at"), t("order_date"))),
  *     Transform("on_time_flag", t => when(datediff(t("shipped_at"), t("order_date")) <= 2, 1).otherwise(0)),
  *   )
  *   .withMeasures(Measure("avg_ship_days", t => sum(t("ship_days"))))  // transform output is a regular column
  * }}}
  *
  * '''Evaluation order:''' transforms apply in declaration order. If transform B
  * references a column added by transform A, declare A first. There is no
  * automatic topological sort — you are responsible for ordering dependencies
  * correctly. (In YAML, the `transforms:` block preserves map order, so the
  * same rule applies.)
  */
final case class Transform(
    name: String,
    expr: SemanticScope => Column,
    description: Option[String] = None,
)

/** Convenience helpers for attaching description and metadata to a [[Measure]]
  * without spelling out `Map(...)` and `Some(...)` each time.
  *
  * {{{
  * import io.semanticdf._
  * import io.semanticdf.MeasureExtra._
  * import org.apache.spark.sql.functions.sum
  *
  * val revenue = describe(
  *   tag(
  *     Measure("revenue", t => sum(t("amount"))),
  *     "owner" -> "finance",
  *     "unit"  -> "USD",
  *   ),
  *   "Total revenue across all orders",
  * )
  * }}}
  *
  * Each helper returns a new `Measure`; the original is unchanged.
  */
object MeasureExtra {
  /** Attach a human-readable description. Surfaced by `describe_model` and OKF
    * catalogs. */
  def describe(m: Measure, description: String): Measure = m.copy(description = Some(description))
  /** Attach one or more metadata key-value pairs. Later pairs with the same
    * key overwrite earlier ones; previously-set keys are preserved. */
  def tag(m: Measure, kvs: (String, String)*): Measure = m.copy(metadata = m.metadata ++ kvs.toMap)
  /** Shortcut for `tag(m, "owner" -> o)`. */
  def owner(m: Measure, o: String): Measure = tag(m, "owner" -> o)
  /** Shortcut for `tag(m, "unit" -> u)`. Common values: `"USD"`, `"count"`, `"ms"`. */
  def unit(m: Measure, u: String): Measure = tag(m, "unit" -> u)
}
