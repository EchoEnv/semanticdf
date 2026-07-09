package io.semantica

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/** Sort-key DSL for [[SemanticTable.orderBy]] / [[SemanticTable.query]] (Phase 5 completion).
  *
  * A bare string is ascending; wrap in [[SortKey.desc]] for descending:
  * {{{
  * st.orderBy("carrier", SortKey.desc("total_passengers"))
  * }}} */
sealed trait SortKey {
  private[semantica] def toColumn: Column
}
object SortKey {
  private[semantica] final case class Asc(name: String)  extends SortKey { def toColumn = col(name).asc }
  private[semantica] final case class Desc(name: String) extends SortKey { def toColumn = col(name).desc }

  /** Explicit ascending key. */
  def asc(name: String): SortKey = Asc(name)
  /** Explicit descending key. */
  def desc(name: String): SortKey = Desc(name)

  /** Implicit `String => SortKey` so `orderBy("carrier", SortKey.desc("x"))` works. */
  implicit def strToSortKey(name: String): SortKey = Asc(name)
}

/** Immutable facade over the root of a semantic op tree (DESIGN §4.1).
  *
  * A `SemanticTable` is *not* a Spark `DataFrame`; it is a deferred, source-agnostic
  * definition that compiles to a DataFrame at an execution terminal. The batch terminal
  * is [[SemanticTable.toDataFrame]] / [[SemanticTable.execute]]; a future streaming
  * terminal (`toStreamingQuery`) is described in ADR 0002 — the same definition, a
  * different sink, mirroring Spark's own `df.write` vs `df.writeStream`.
  */
final class SemanticTable private[semantica] (
    private[semantica] val root: SemanticOp,
    private[semantica] val postAggPredicates: List[Predicate] = Nil,
) {

  /** Batch terminal (DESIGN §4.5).
    *
    * Compiles the op tree against `spark` and returns the resulting `DataFrame`.
    * Recompiles on every call; never caches the result internally (DESIGN §4.4).
    */
  def toDataFrame(spark: SparkSession): DataFrame =
    root.compile(spark)

  /** Fluent-chain alias for [[toDataFrame]]. */
  def execute(spark: SparkSession): DataFrame = toDataFrame(spark)

  // -------------------------------------------------------------------------
  // Model extension
  // -------------------------------------------------------------------------

  /** Extend the model with dimensions. Handles single-table and joined roots (Phase 4).
    * Returns a new [[SemanticTable]] (immutable). */
  def withDimensions(dims: Dimension*): SemanticTable = {
    val extra = dims.map(d => d.name -> d).toMap
    root match {
      case t: SemanticTableOp =>
        new SemanticTable(t.copy(dimensions = t.dimensions ++ extra), postAggPredicates)

      case j: SemanticJoinOp =>
        // Pass extra dimensions so mergedModel includes them.
        val updatedJoin = SemanticJoinOp(
          left   = j.left,
          right  = j.right,
          on     = j.on,
          cardinality = j.cardinality,
          leftRoot  = j.leftRoot,
          rightRoot = j.rightRoot,
          extraDimensions = j.extraDimensions ++ extra,
          extraMeasures   = j.extraMeasures,
        )
        new SemanticTable(updatedJoin, postAggPredicates)

      // Passthrough ops (Phase 5/6): recurse to the underlying table/join, then re-wrap.
      // Lets a user (or query()) chain withDimensions after where()/orderBy()/limit().
      case SemanticFilterOp(src, pred) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticFilterOp(inner.root, pred), postAggPredicates)
      case SemanticOrderByOp(src, keys) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticOrderByOp(inner.root, keys), postAggPredicates)
      case SemanticLimitOp(src, n) =>
        val inner = new SemanticTable(src).withDimensions(dims: _*)
        new SemanticTable(SemanticLimitOp(inner.root, n), postAggPredicates)

      case _ =>
        throw new IllegalStateException(
          s"withDimensions: unexpected root type ${root.getClass.getSimpleName}"
        )
    }
  }

  /** Extend the model with measures. Handles single-table and joined roots (Phase 4).
    * Returns a new [[SemanticTable]] (immutable). */
  def withMeasures(measures: Measure*): SemanticTable = {
    val extra = measures.map(m => m.name -> m).toMap
    root match {
      case t: SemanticTableOp =>
        new SemanticTable(t.copy(measures = t.measures ++ extra), postAggPredicates)

      case j: SemanticJoinOp =>
        val updatedJoin = SemanticJoinOp(
          left   = j.left,
          right  = j.right,
          on     = j.on,
          cardinality = j.cardinality,
          leftRoot  = j.leftRoot,
          rightRoot = j.rightRoot,
          extraDimensions = j.extraDimensions,
          extraMeasures   = j.extraMeasures ++ extra,
        )
        new SemanticTable(updatedJoin, postAggPredicates)

      // Passthrough ops (Phase 5/6): recurse to the underlying table/join, then re-wrap.
      case SemanticFilterOp(src, pred) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticFilterOp(inner.root, pred), postAggPredicates)
      case SemanticOrderByOp(src, keys) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticOrderByOp(inner.root, keys), postAggPredicates)
      case SemanticLimitOp(src, n) =>
        val inner = new SemanticTable(src).withMeasures(measures: _*)
        new SemanticTable(SemanticLimitOp(inner.root, n), postAggPredicates)

      case _ =>
        throw new IllegalStateException(
          s"withMeasures: unexpected root type ${root.getClass.getSimpleName}"
        )
    }
  }

  // -------------------------------------------------------------------------
  // Phase 4: Joins (DESIGN §7)
  // -------------------------------------------------------------------------

  /** Join with a one-to-one / parent-child relationship (`join_one`).
    *
    * Aggregation after the join is safe — fact rows are not multiplied by dimension rows.
    * Also eligible for deferred-join optimization (pre-agg fact table + left-join dimension
    * table) when only dimensions from the right table are queried.
    *
    * @param other the right-side semantic table
    * @param on    the equi-join predicate. Use `l("col") === r("col")` where `l` and `r`
    *              are the join-side proxies passed to the lambda. Both sides use bare column
    *              names — the proxy resolves them against the respective DataFrame.
    *
    * @example
    * {{{
    * val orders = toSemanticTable(ordersDf, name = Some("orders"))
    * val customers = toSemanticTable(customersDf, name = Some("customers"))
    *
    * val joined = orders.join_one(customers, (l, r) => l("customer_id") === r("id"))
    *   .withDimensions(Dimension("name", t => t("customers.name")))
    *   .withMeasures(Measure("total_amount", t => sum(t("amount"))))
    *
    * joined.groupBy("customers.name").aggregate("total_amount").execute(spark)
    * }}}
    */
  def join_one(
      other: SemanticTable,
      on: (JoinSide, JoinSide) => Column,
  ): SemanticTable = {
    val leftRoot  = requireRoot("join_one (left)")
    val rightRoot = other.requireRoot("join_one (right)")
    val join = SemanticJoinOp(
      left   = leftRoot,
      right  = rightRoot,
      on     = on,
      cardinality = JoinCardinality.One,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
    )
    new SemanticTable(join)
  }

  /** Join with a one-to-many / fan-out relationship (`join_many`).
    *
    * This is the primary join for star-schema models (fact → dimension).
    * Each source table's measures are **pre-aggregated at the join-key grain before
    * joining**, preventing the fact-row multiplication that would otherwise inflate
    * SUM/COUNT. This is BSL's safe-aggregation pattern (DESIGN §7.1).
    *
    * Leaf-level measures (e.g. `"line_items.qty_sum"`) are unaffected by fan-out since
    * they are computed at their own source grain before the join.
    *
    * @param other the right-side semantic table (typically the many side)
    * @param on    the equi-join predicate, same form as [[join_one]]
    *
    * @example
    * {{{
    * val orders = toSemanticTable(ordersDf, name = Some("orders"))
    *   .withDimensions(Dimension("customer_id", t => t("customer_id")))
    *   .withMeasures(Measure("total_amount", t => sum(t("amount"))))
    *
    * val items = toSemanticTable(lineItemsDf, name = Some("items"))
    *   .withDimensions(Dimension("order_id", t => t("order_id")))
    *   .withMeasures(Measure("item_count", t => count(lit(1))))
    *
    * // Pre-agg at join-key grain: orders → customer_id, items → order_id, then join.
    * // total_amount = 100 + 200 + 150 = 450 (NOT inflated by the 1:many item rows).
    * orders.join_many(items, (l, r) => l("order_id") === r("order_id"))
    *   .groupBy("orders.customer_id")
    *   .aggregate("orders.total_amount")
    *   .execute(spark)
    * }}}
    */
  def join_many(
      other: SemanticTable,
      on: (JoinSide, JoinSide) => Column,
  ): SemanticTable = {
    val leftRoot  = requireRoot("join_many (left)")
    val rightRoot = other.requireRoot("join_many (right)")
    val join = SemanticJoinOp(
      left   = leftRoot,
      right  = rightRoot,
      on     = on,
      cardinality = JoinCardinality.Many,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
    )
    new SemanticTable(join)
  }

  /** Cross join (Cartesian product) with another semantic table (`join_cross`).
    *
    * Every row in `this` is paired with every row in `other`. Use with caution —
    * row counts multiply.
    */
  def join_cross(other: SemanticTable): SemanticTable = {
    val leftRoot  = requireRoot("join_cross (left)")
    val rightRoot = other.requireRoot("join_cross (right)")
    val join = SemanticJoinOp(
      left   = leftRoot,
      right  = rightRoot,
      on     = (_, _) => throw new IllegalStateException("Cross join has no predicate."),
      cardinality = JoinCardinality.Cross,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
    )
    new SemanticTable(join)
  }

  // -------------------------------------------------------------------------
  // Phase 5: Filtering (WHERE / HAVING, DESIGN §6.5)
  // -------------------------------------------------------------------------

  /** Apply a filter predicate with automatic WHERE/HAVING routing.
    *
    * Predicates over **dimensions** are applied pre-aggregation (WHERE — they filter
    * base rows). Predicates over **measures** are applied post-aggregation (HAVING —
    * they filter aggregated rows). `And` compounds are split per-condition; `Or`/
    * `Not` mixing dimension and measure conditions stay whole (post-agg, since they
    * cannot be evaluated independently).
    *
    * Use [[having]] to force a predicate to post-aggregation regardless of routing.
    *
    * @example
    * {{{
    * import Predicate._
    * st.where("carrier" === "AA")                      // WHERE
    * st.where("total_passengers" > 600)                 // HAVING
    * st.where(("carrier" === "AA") and ("total" > 100)) // split: WHERE + HAVING
    * }}}
    */
  def where(pred: Predicate): SemanticTable = {
    val knownMeasures = resolveAllMeasureNames
    val (pre, post) = Predicate.splitFilter(pred, knownMeasures)
    val newRoot = pre.foldLeft(root) { (r, p) =>
      SemanticFilterOp(r, p)
    }
    new SemanticTable(newRoot, postAggPredicates ++ post)
  }

  /** Apply a filter predicate explicitly as post-aggregation (HAVING).
    *
    * Use when you want a dimension filter to apply after aggregation (rare, but
    * sometimes needed when the dimension is derived from a measure). */
  def having(pred: Predicate): SemanticTable =
    new SemanticTable(root, postAggPredicates :+ pred)

  // -------------------------------------------------------------------------
  // Phase 5 completion: order_by / limit / query
  // -------------------------------------------------------------------------

  /** Order the result by one or more sort keys.
    *
    * Bare strings are ascending; use [[SortKey.desc]] for descending:
    * {{{
    * st.orderBy("carrier", SortKey.desc("total_passengers"))
    * }}}
    *
    * Typically chained after `aggregate()`. Composes with [[limit]]. */
  def orderBy(keys: SortKey*): SemanticTable =
    new SemanticTable(SemanticOrderByOp(root, keys), postAggPredicates)

  /** Limit the result to the first `n` rows. Composes with [[orderBy]]. */
  def limit(n: Int): SemanticTable =
    new SemanticTable(SemanticLimitOp(root, n), postAggPredicates)

  /** One-shot bundled query (Phase 5 completion).
    *
    * Pure sugar over the fluent API — chains `where → groupBy → aggregate[having] →
    * orderBy → limit`. Useful for parameterized / programmatic query building where
    * the fluent chain would be awkward. All parameters except `measures` are optional.
    *
    * @example
    * {{{
    * st.query(
    *   measures   = Seq("total_passengers", "pct_of_total"),
    *   dimensions = Seq("carrier"),
    *   where      = Some("carrier" in ("AA", "UA")),
    *   orderBy    = Seq(SortKey.desc("total_passengers")),
    *   limit      = Some(10),
    * )
    * }}} */
  def query(
      measures: Iterable[String],
      dimensions: Iterable[String] = Nil,
      where: Option[Predicate] = None,
      having: Option[Predicate] = None,
      orderBy: Iterable[SortKey] = Nil,
      limit: Option[Int] = None,
      timeGrain: Option[String] = None,
      timeGrains: Map[String, String] = Map.empty,
      timeRange: Option[(String, String)] = None,
  ): SemanticTable = {
    if (timeGrain.isDefined && timeGrains.nonEmpty)
      throw new IllegalArgumentException(
        "Cannot specify both 'timeGrain' and 'timeGrains'. Use 'timeGrain' for a single " +
          "grain applied to all time dimensions, or 'timeGrains' for per-dimension grains.")
    var t = this
    // Phase 6: time_range first (filters rows by raw timestamp, pre-truncation).
    timeRange.foreach { case (start, end) =>
      val td = findTimeDimension(dimensions)
        .getOrElse(throw new IllegalArgumentException(
          "timeRange requires a time dimension among the query dimensions. " +
            "Mark one with Dimension.time(...) and include it in `dimensions`."))
      // Build Compare predicates explicitly: `td` is a String-typed dimension name, and
      // Scala's Ordered[String] member >= would shadow the Predicate DSL implicit, so we
      // bypass it. time_range filters on the raw column named `td` (pre-truncation).
      val rangePred = Predicate.Compare("ge", td, start).and(Predicate.Compare("le", td, end))
      t = t.where(rangePred)
    }
    // Phase 6: grain truncation overrides each time dimension's expr.
    val grainMap: Map[String, String] =
      if (timeGrains.nonEmpty) timeGrains
      else timeGrain.map(g => timeDimensionsAmong(dimensions).map(_ -> g).toMap).getOrElse(Map.empty)
    grainMap.foreach { case (dim, g) => t = t.atTimeGrain(dim, g) }
    where.foreach(p  => t = t.where(p))
    having.foreach(p => t = t.having(p))
    var result: SemanticTable = t.groupBy(dimensions.toSeq: _*).aggregate(measures.toSeq: _*)
    if (orderBy.nonEmpty) result = result.orderBy(orderBy.toSeq: _*)
    limit.foreach(n => result = result.limit(n))
    result
  }

  // -------------------------------------------------------------------------
  // Group-by / aggregate
  // -------------------------------------------------------------------------

  /** Begin a group-by + aggregate. Returns a builder whose `aggregate(...)` produces
    * the aggregated [[SemanticTable]]. */
  def groupBy(keys: String*): SemanticGroupBy =
    new SemanticGroupBy(root, keys, postAggPredicates)

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  /** Resolve the set of all known measure names on this table's model.
    * Used by [[where]] to route filters (measure refs → HAVING, else → WHERE). */
  /** Apply a time grain to a time dimension (Phase 6, DESIGN §6.6).
    *
    * Truncates `dimName`'s expression to `grain` (e.g. `"month"`) so group-by buckets
    * at that granularity. Validates the dimension is a time dimension and that `grain`
    * is not finer than its declared `smallestTimeGrain`.
    *
    * Implementation mirrors BSL: override the dimension's `expr` with a truncated
    * version via [[withDimensions]] — no new op node, reuses all existing machinery.
    * Filtering (time_range) stays on the raw column since filters compile against the
    * base scope; only grouping sees the truncation. */
  def atTimeGrain(dimName: String, grain: String): SemanticTable = {
    val unit = TimeGrain.normalize(grain)
    val dim  = resolveDimension(dimName).getOrElse(throw new IllegalArgumentException(
      s"atTimeGrain: dimension '$dimName' not found on this table."))
    if (!dim.isTimeDimension)
      throw new IllegalArgumentException(
        s"atTimeGrain: '$dimName' is not a time dimension. " +
          "Declare it with Dimension.time(...) to enable grain truncation.")
    TimeGrain.validateNotFiner(unit, dim.smallestTimeGrain, dimName)
    val truncated: Dimension = dim.copy(expr = (scope: SemanticScope) =>
      TimeGrain.truncate(unit, dim.expr(scope)))
    withDimensions(truncated)
  }

  /** Find the first time dimension among `dims`, by name. */
  private def findTimeDimension(dims: Iterable[String]): Option[String] =
    dims.find(name => resolveDimension(name).exists(_.isTimeDimension))

  /** All time dimensions among `dims`, by name. */
  private def timeDimensionsAmong(dims: Iterable[String]): Seq[String] =
    dims.filter(name => resolveDimension(name).exists(_.isTimeDimension)).toSeq

  /** Look up a dimension by name across single-table and joined models. */
  private def resolveDimension(name: String): Option[Dimension] = root match {
    case t: SemanticTableOp => t.dimensions.get(name)
    case j: SemanticJoinOp  => j.mergedModel.dimensions.get(name)
    case SemanticFilterOp(src, _)     => new SemanticTable(src).resolveDimension(name)
    case SemanticOrderByOp(src, _)    => new SemanticTable(src).resolveDimension(name)
    case SemanticLimitOp(src, _)      => new SemanticTable(src).resolveDimension(name)
    case _ => SemanticOp.rootModel(root).flatMap(_.dimensions.get(name))
  }

  private def resolveAllMeasureNames: Set[String] = root match {
    case t: SemanticTableOp => t.measures.keySet
    case j: SemanticJoinOp  => j.mergedModel.measures.keySet
    case SemanticFilterOp(src, _) =>
      // Unwrap filters to find the underlying model.
      new SemanticTable(src).resolveAllMeasureNames
    case _ =>
      SemanticOp.rootModel(root).map(_.measures.keySet).getOrElse(Set.empty)
  }

  private def rootModel: SemanticTableOp = root match {
    case t: SemanticTableOp => t
    case _ =>
      SemanticOp.rootModel(root).getOrElse(
        throw new IllegalStateException(
          s"SemanticTable root is not a SemanticTableOp: ${root.getClass.getSimpleName}. " +
            "Joins produce a SemanticJoinOp; use withDimensions/withMeasures on the " +
            "result to add dimensions/measures before aggregating."
        )
      )
  }

  private def requireRoot(label: String): SemanticTableOp =
    root match {
      case t: SemanticTableOp => t
      case j: SemanticJoinOp =>
        throw new IllegalArgumentException(
          s"$label: the left side is already a joined table. " +
            "Chain joins by calling join_one/join_many on the result: " +
            "table.join_one(first, on).join_one(second, on2)."
        )
      case a: SemanticAggregateOp =>
        throw new IllegalArgumentException(
          s"$label: cannot join after aggregate(). Join tables first, then call groupBy()."
        )
    }

  override def toString: String = s"SemanticTable(${root.getClass.getSimpleName})"
}

/** Builder produced by [[SemanticTable.groupBy]]. `aggregate(measure names...)` compiles
  * to a [[SemanticAggregateOp]] wrapped in a [[SemanticTable]].
  *
  * Post-aggregation predicates (HAVING) accumulated via [[SemanticTable.where]] /
  * [[SemanticTable.having]] are applied by wrapping the aggregate result in
  * [[SemanticFilterOp]] nodes. */
final class SemanticGroupBy private[semantica] (
    source: SemanticOp,
    keys: Seq[String],
    postAggPredicates: List[Predicate] = Nil,
) {
  def aggregate(measures: String*): SemanticTable = {
    var op: SemanticOp = SemanticAggregateOp(source, keys, measures)
    // Wrap with post-agg filters (HAVING). Each is a SemanticFilterOp on the aggregate.
    postAggPredicates.foreach { p => op = SemanticFilterOp(op, p) }
    new SemanticTable(op)
  }
}
