package io.semantica

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import scala.jdk.CollectionConverters._

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

  /** Read the column-name field of any SortKey (private to avoid exposing the sealed
    * cases to public API). Used by [[SemanticTable.explainSemantic]]. */
  private[semantica] def nameOf(k: SortKey): String = k match {
    case Asc(n)  => n
    case Desc(n) => n
    case _       => ""
  }

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
  // Observability (Phase B)
  // -------------------------------------------------------------------------

  /** Summarize the planned execution path without running anything.
    *
    * Shows the op-tree shape, dimensions, measures, joins, filters, and the
    * aggregate plan — everything needed to understand what [[toDataFrame]] will do.
    * Classification decisions (base vs calc, topological layers) are logged by
    * [[SemanticLogger]] and appear in the output when Spark DEBUG logging is enabled
    * for the `io.semantica` logger.
    *
    * Use [[explain(spark)]] to see Spark's physical plan after compilation.
    *
    * @return a human-readable plan summary
    */
  def explain(): String = {
    val sb = new StringBuilder
    sb.append("semantica plan:\n")
    explainNode(root, sb, "  ")
    sb.toString
  }

  private def explainNode(op: SemanticOp, sb: StringBuilder, indent: String): Unit = op match {
    case t: SemanticTableOp =>
      sb.append(s"${indent}table: ${t.name.getOrElse("(anonymous)")} " +
        s"[${t.table.columns.size} columns]\n")
      if (t.dimensions.nonEmpty)
        sb.append(s"${indent}  dimensions: ${t.dimensions.keys.mkString(", ")}\n")
      if (t.measures.nonEmpty) {
        sb.append(s"${indent}  measures:\n")
        t.measures.values.foreach(m =>
          sb.append(s"${indent}    ${m.name}: ${m.getClass.getSimpleName.replace("$", "")}"))
        sb.append("\n")
      }

    case j: SemanticJoinOp =>
      sb.append(s"${indent}join(${j.cardinality})\n")
      sb.append(s"${indent}  left:\n")
      explainNode(j.left, sb, indent + "    ")
      sb.append(s"${indent}  right:\n")
      explainNode(j.right, sb, indent + "    ")
      if (j.extraDimensions.nonEmpty)
        sb.append(s"${indent}  extra dimensions: ${j.extraDimensions.keys.mkString(", ")}\n")
      if (j.extraMeasures.nonEmpty)
        sb.append(s"${indent}  extra measures: ${j.extraMeasures.keys.mkString(", ")}\n")

    case a: SemanticAggregateOp =>
      sb.append(s"${indent}aggregate(keys=[${a.keys.mkString(", ")}])\n")
      sb.append(s"${indent}  measures: [${a.measureNames.mkString(", ")}]\n")
      sb.append(s"${indent}  source:\n")
      explainNode(a.source, sb, indent + "    ")

    case SemanticFilterOp(src, pred) =>
      sb.append(s"${indent}filter(${pred.describe})\n")
      sb.append(s"${indent}  source:\n")
      explainNode(src, sb, indent + "    ")

    case SemanticOrderByOp(src, keys) =>
      sb.append(s"${indent}orderBy(${keys.map(_.toString).mkString(", ")})\n")
      sb.append(s"${indent}  source:\n")
      explainNode(src, sb, indent + "    ")

    case SemanticLimitOp(src, n) =>
      sb.append(s"${indent}limit($n)\n")
      sb.append(s"${indent}  source:\n")
      explainNode(src, sb, indent + "    ")
  }

  /** Print the Spark physical plan after compiling the op tree.
    *
    * Calls `toDataFrame(spark).explain()` and returns the explain string.
    * This is the "real" plan — Catalyst-optimized, with actual column names,
    * shuffle/partitions info, and broadcast hints visible.
    *
    * Unlike [[explain()]] which shows the semantica op tree without compiling,
    * this method compiles the full plan and asks Spark to explain it.
    *
    * @param spark the active SparkSession
    * @return Spark's explain output string
    */
  def explain(spark: SparkSession): String = {
    val df = toDataFrame(spark)
    df.queryExecution.explainString(
      org.apache.spark.sql.execution.ExplainMode.fromString("simple")
    )
  }

  /** Human-readable plan combining semantic intent + Spark's physical plan (Tier 1.5,
    * roadmap §1.5).
    *
    * Unlike [[explain()]] which is the op-tree shape only, or [[explain(spark)]] which
    * is just Catalyst output, this method explains *why* semantica routed things the
    * way it did: where each filter went (WHERE vs HAVING), which transitive measures
    * were pulled in, what time grains are valid, which joins compiled pre- vs post-agg.
    *
    * Sections, in order:
    *  1. ROUTING          — every filter, where it compiled, which field it references
    *  2. TRANSITIVE DEPS  — measures requested, transitively-pulled, and skipped
    *  3. DIMENSIONS       — list with time/pii/entity flags
    *  4. MEASURES         — list with base/calc classification
    *  5. JOINS            — cardinality, pre/post-agg strategy
    *  6. WARNINGS         — anything notable (calc cycles, time-grain risk, etc.)
    *  7. SPARK PLAN       — `df.explain()` output (only when `spark` is provided)
    *
    * Compile is forced iff `spark` is provided — pass `null` for a static-only view
    * (the cost is just walking the op tree).
    *
    * @param spark optional SparkSession. Required only for section 7 (Spark plan).
    *              Pass `null` to skip compilation.
    * @return multi-line plan summary
    */
  def explainSemantic(spark: SparkSession): String = {
    val renderer = new SemanticPlanRenderer(this)
    renderer.render(spark)
  }

  /** Return a DataFrame describing every field (dimensions + measures) in this model.
    *
    * This is the analogue of Spark's `df.schema` for a semantic model — it flattens
    * all model metadata into a queryable DataFrame so you can explore, filter, and
    * catalog the model programmatically.
    *
    * {{{
    *   val catalog = model.schema(spark)
    *
    *   // Find all PII fields
    *   catalog.filter(c => c("metadata_keys").contains("pii")).show()
    *
    *   // List all measures owned by finance
    *   catalog.filter(c => c("metadata_owner") === "finance").show()
    *
    *   // Export the full schema to a Delta table
    *   catalog.write.format("delta").save("_semantica/model_schema")
    * }}}
    *
    * The DataFrame has one row per field with these columns:
    *   - `model_name`: source table / joined model name
    *   - `model_description`: human description of the model
    *   - `field_name`: dimension or measure name
    *   - `field_type`: `"dimension"` or `"measure"`
    *   - `description`: the field's description (empty if none)
    *   - `metadata_keys`: comma-separated list of metadata keys
    *   - `metadata_values`: comma-separated list of metadata values (aligned with keys)
    *   - `is_entity`: true for entity (join-key) dimensions
    *   - `is_time_dimension`: true for time/timestamp dimensions
    *   - `smallest_grain`: for time dims, the finest supported time grain
    *   - `join_alias`: if this field comes from a joined table, the join alias
    *
    * @param spark the active SparkSession (used only to create the result DataFrame)
    */
  def schema(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val rows = collectSchemaFields(root, None, None)

    val resultSchema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("model_name",          org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("model_description",  org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("field_name",          org.apache.spark.sql.types.StringType,  nullable = false),
      org.apache.spark.sql.types.StructField("field_type",          org.apache.spark.sql.types.StringType,  nullable = false),
      org.apache.spark.sql.types.StructField("description",         org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("metadata_keys",        org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("metadata_values",      org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("is_entity",            org.apache.spark.sql.types.BooleanType, nullable = false),
      org.apache.spark.sql.types.StructField("is_time_dimension",    org.apache.spark.sql.types.BooleanType, nullable = false),
      org.apache.spark.sql.types.StructField("smallest_grain",       org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("join_source",          org.apache.spark.sql.types.StringType,  nullable = true),
      org.apache.spark.sql.types.StructField("join_cardinality",     org.apache.spark.sql.types.StringType,  nullable = true),
    ))

    val sparkRows = rows.map { case (mName, mDesc, fName, fType, desc, mKeys, mVals, isEnt, isTime, grain, jSrc, jCard) =>
      org.apache.spark.sql.Row(
        mName.orNull, mDesc.orNull, fName, fType, desc.orNull,
        if (mKeys.isEmpty) null else mKeys,
        if (mVals.isEmpty) null else mVals,
        isEnt: java.lang.Boolean, isTime: java.lang.Boolean,
        grain.orNull, jSrc.orNull, jCard.orNull,
      )
    }
    spark.createDataFrame(sparkRows.asJava, resultSchema)
  }

  /** Recursively collect schema fields from the op tree. Returns flat list of row tuples. */
  private def collectSchemaFields(
      op: SemanticOp,
      joinSource: Option[String],
      joinCardinality: Option[String],
  ): List[(Option[String], Option[String], String, String, Option[String], String, String, Boolean, Boolean, Option[String], Option[String], Option[String])] = op match {
    case t: SemanticTableOp =>
      val modelName = t.name.orElse(Some("anonymous"))
      val modelDesc = t.description
      val dimRows = t.dimensions.values.map(d =>
        (modelName, modelDesc, d.name, "dimension",
          d.description, d.metadata.keys.mkString(","), d.metadata.values.mkString(","),
          d.isEntity, d.isTimeDimension, d.smallestTimeGrain, joinSource, joinCardinality)
      ).toList
      val measRows = t.measures.values.map(m =>
        (modelName, modelDesc, m.name, "measure",
          m.description, m.metadata.keys.mkString(","), m.metadata.values.mkString(","),
          false, false, None, joinSource, joinCardinality)
      ).toList
      dimRows ::: measRows

    case j: SemanticJoinOp =>
      val leftFields  = collectSchemaFields(j.left,  None,             None)
      val rightSource = j.rightRoot.name.orElse(Some("joined"))
      val rightFields = collectSchemaFields(j.right, rightSource, Some(j.cardinality.toString))
      leftFields ::: rightFields

    case f: SemanticFilterOp  => collectSchemaFields(f.source, joinSource, joinCardinality)
    case o: SemanticOrderByOp => collectSchemaFields(o.source, joinSource, joinCardinality)
    case l: SemanticLimitOp   => collectSchemaFields(l.source, joinSource, joinCardinality)
    case a: SemanticAggregateOp => collectSchemaFields(a.source, joinSource, joinCardinality)
  }

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
      s"atTimeGrain: dimension '$dimName' not found on this table.${closestMatch(dimName, dimensions.keys).map(c => s" Did you mean: '$c'?").getOrElse("")}"))
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

  // -----------------------------------------------------------------------
  // Catalog accessors (unblocks first-consumer: explore a SemanticTable)
  // -----------------------------------------------------------------------

  /** All dimensions declared on this semantic table. */
  def dimensions: Map[String, Dimension] = resolveRootModel.dimensions

  /** All measures declared on this semantic table (base and calc). */
  def measures: Map[String, Measure] = resolveRootModel.measures

  /** Look up a dimension by name. */
  def findDimension(name: String): Option[Dimension] = dimensions.get(name)

  /** Look up a measure by name. */
  def findMeasure(name: String): Option[Measure] = measures.get(name)

  // -------------------------------------------------------------------------
  // Metastore integration (unblocks BI tools: query via Spark SQL)
  // -------------------------------------------------------------------------

  /** Compile this semantic table and register it as a Spark temporary view.
    *
    * After registration, any Spark SQL query can reference `name` as a table:
    * {{{
    * st.createOrReplaceTempView("flights")
    * spark.sql("SELECT carrier, total_passengers FROM flights WHERE carrier = 'AA'")
    * }}}
    *
    * The view is session-scoped and disappears when the SparkSession stops.
    * For a global view, use [[createOrReplaceGlobalTempView]].
    *
    * @param name the view name (must be a valid Spark identifier)
    */
  def createOrReplaceTempView(name: String)(implicit spark: SparkSession): Unit =
    toDataFrame(spark).createOrReplaceTempView(name)

  /** Compile this semantic table and register it as a session-scoped temp view.
    *
    * Unlike [[createOrReplaceTempView]], this throws if `name` already exists.
    *
    * @param name the view name
    */
  def createTempView(name: String)(implicit spark: SparkSession): Unit =
    toDataFrame(spark).createTempView(name)

  /** Compile this semantic table and register it as a global Spark temporary view.
    *
    * Global views are stored in the global_temp database and persist across sessions
    * within the same application:
    * {{{
    * st.createOrReplaceGlobalTempView("flights")
    * spark.sql("SELECT * FROM global_temp.flights")
    * }}}
    *
    * @param name the view name
    */
  def createOrReplaceGlobalTempView(name: String)(implicit spark: SparkSession): Unit =
    toDataFrame(spark).createOrReplaceGlobalTempView(name)

  // -------------------------------------------------------------------------
  // Typed result schema (unblocks first-consumer: preview output before run)
  // -------------------------------------------------------------------------

  /** Compile this semantic table and return the output schema as a StructType.
    *
    * No rows are executed — only the plan is built and resolved to a schema.
    * Use this to discover what columns `.execute(spark)` will produce before
    * running it, or to drive code generation, validation, or documentation.
    *
    * @param spark the active SparkSession
    * @return the output schema (dimension columns + measure columns)
    */
  def previewSchema(spark: SparkSession): StructType =
    toDataFrame(spark).schema

  // -------------------------------------------------------------------------
  // Internals (reordered for readability)
  // -------------------------------------------------------------------------

  private def resolveRootModel: MergedSemanticModel = root match {
    case t: SemanticTableOp => MergedSemanticModel(t.dimensions, t.measures)
    case j: SemanticJoinOp  => j.mergedModel
    case SemanticAggregateOp(src, _, _) =>
      new SemanticTable(src).resolveRootModel
    case SemanticFilterOp(src, _)  => new SemanticTable(src).resolveRootModel
    case SemanticOrderByOp(src, _) => new SemanticTable(src).resolveRootModel
    case SemanticLimitOp(src, _)   => new SemanticTable(src).resolveRootModel
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

// =============================================================================
// SemanticPlanRenderer — Tier 1.5 explain-semantic renderer
// =============================================================================

/* Walks a SemanticTable's op tree and produces a multi-section human-readable plan
 * explaining where every predicate went, what dependencies were pulled in, what
 * joins compiled pre/post-agg, and which Spark plan was generated.
 *
 * Why a separate class:
 *   - keeps `SemanticTable` small (already 770+ lines)
 *   - isolates the rendering logic so it can be unit-tested independently
 *   - it's stateless apart from the captured `SemanticTable` reference
 *
 * Output sections (separated by horizontal rules):
 *   1. PLAN SUMMARY      - one-line description of the query
 *   2. SEMANTIC ROUTING  - filter decisions (WHERE/HAVING) with reasons
 *   3. TRANSITIVE DEPS   - measures requested vs pulled in vs skipped
 *   4. DIMENSIONS        - with time/pii/entity flags
 *   5. MEASURES          - with base/calc classification + reasons
 *   6. JOINS             - cardinality + pre/post-agg strategy
 *   7. WARNINGS          - notable concerns (only if non-empty)
 *   8. SPARK PLAN        - df.explain() output (only when spark is provided)
 */
private class SemanticPlanRenderer(st: SemanticTable) {

  /** Render the multi-section plan. */
  def render(spark: SparkSession): String = {
    val sb = new StringBuilder

    sb.append(renderSummary())
    sb.append(hr()).append(renderRouting())
    sb.append(hr()).append(renderTransitiveDeps())
    sb.append(hr()).append(renderDimensions())
    sb.append(hr()).append(renderMeasures())
    sb.append(hr()).append(renderJoins())

    val warnings = collectWarnings()
    if (warnings.nonEmpty) {
      sb.append(hr()).append(renderWarnings(warnings))
    }

    if (spark != null) {
      sb.append(hr()).append(renderSparkPlan(spark))
    }

    sb.toString
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def hr(): String = "\n" + ("─" * 50) + "\n"
  private def heading(text: String): String = s"$text\n${"─" * text.length}\n"
  private def indent(s: String, n: Int = 2): String =
    s.linesIterator.map(line => if (line.isEmpty) line else (" " * n) + line).mkString("\n")

  /** Walk the op tree and return all referenced field names (dims + measures). */
  private def referencedFields(): Set[String] = {
    val acc = scala.collection.mutable.Set[String]()
    def walk(op: SemanticOp): Unit = op match {
      case t: SemanticTableOp =>
        t.dimensions.keys.foreach(acc.add)
        t.measures.keys.foreach(acc.add)
      case j: SemanticJoinOp =>
        j.extraDimensions.keys.foreach(acc.add)
        j.extraMeasures.keys.foreach(acc.add)
        walk(j.left); walk(j.right)
      case a: SemanticAggregateOp =>
        a.keys.foreach(acc.add)
        a.measureNames.foreach(acc.add)
        walk(a.source)
      case SemanticFilterOp(src, pred) =>
        pred.fields.foreach(acc.add)
        walk(src)
      case SemanticOrderByOp(src, _)   => walk(src)
      case SemanticLimitOp(src, _)     => walk(src)
    }
    walk(st.root)
    acc.toSet
  }

  /** All measure names reachable from the root, in stable order. */
  private def allMeasures(): Seq[(String, Measure)] = {
    val acc = scala.collection.mutable.LinkedHashMap.empty[String, Measure]
    def walk(op: SemanticOp): Unit = op match {
      case t: SemanticTableOp =>
        t.measures.foreach { case (n, m) => acc.update(n, m) }
      case j: SemanticJoinOp =>
        j.extraMeasures.foreach { case (n, m) => acc.update(n, m) }
        walk(j.left); walk(j.right)
      case a: SemanticAggregateOp     => walk(a.source)
      case SemanticFilterOp(src, _)   => walk(src)
      case SemanticOrderByOp(src, _)  => walk(src)
      case SemanticLimitOp(src, _)    => walk(src)
    }
    walk(st.root)
    acc.toSeq
  }

  /** All dimensions reachable from the root, in stable order. */
  private def allDimensions(): Seq[(String, Dimension)] = {
    val acc = scala.collection.mutable.LinkedHashMap.empty[String, Dimension]
    def walk(op: SemanticOp): Unit = op match {
      case t: SemanticTableOp =>
        t.dimensions.foreach { case (n, d) => acc.update(n, d) }
      case j: SemanticJoinOp =>
        j.extraDimensions.foreach { case (n, d) => acc.update(n, d) }
        walk(j.left); walk(j.right)
      case a: SemanticAggregateOp     => walk(a.source)
      case SemanticFilterOp(src, _)   => walk(src)
      case SemanticOrderByOp(src, _)  => walk(src)
      case SemanticLimitOp(src, _)    => walk(src)
    }
    walk(st.root)
    acc.toSeq
  }

  /** All join operations in the op tree. */
  private def allJoins(): Seq[SemanticJoinOp] = {
    val acc = scala.collection.mutable.ListBuffer.empty[SemanticJoinOp]
    def walk(op: SemanticOp): Unit = op match {
      case j: SemanticJoinOp =>
        acc += j; walk(j.left); walk(j.right)
      case a: SemanticAggregateOp     => walk(a.source)
      case SemanticFilterOp(src, _)   => walk(src)
      case SemanticOrderByOp(src, _)  => walk(src)
      case SemanticLimitOp(src, _)    => walk(src)
      case _: SemanticTableOp         => // leaf
    }
    walk(st.root)
    acc.toSeq
  }

  /** All filter operations, in op-tree order (top-down). */
  private def allFilters(): Seq[(SemanticFilterOp, Boolean)] = {
    // Boolean = "is HAVING (post-agg)" — true iff the filter's own source is an
    // aggregate (matches SemanticFilterOp.compile's runtime check).
    val acc = scala.collection.mutable.ListBuffer.empty[(SemanticFilterOp, Boolean)]
    def walk(op: SemanticOp): Unit = op match {
      case f @ SemanticFilterOp(src, pred) =>
        val isHaving = src match {
          case _: SemanticAggregateOp => true
          case _                      => false
        }
        acc += ((f, isHaving))
        walk(src)
      case j: SemanticJoinOp =>
        walk(j.left); walk(j.right)
      case a: SemanticAggregateOp    => walk(a.source)
      case SemanticOrderByOp(src, _) => walk(src)
      case SemanticLimitOp(src, _)   => walk(src)
      case _: SemanticTableOp        => // leaf
    }
    walk(st.root)
    acc.toSeq
  }

  // ---------------------------------------------------------------------------
  // Section renderers
  // ---------------------------------------------------------------------------

  private def renderSummary(): String = {
    val sb = new StringBuilder
    sb.append(heading("PLAN SUMMARY"))

    // Table name(s) — for joins, all source tables are listed (root first).
    val tableNames = allTableNames
    val tableLabel = if (tableNames.isEmpty) "(unnamed)"
                     else if (tableNames.size == 1) tableNames.head
                     else tableNames.mkString(" + ")
    sb.append(s"  table:   $tableLabel\n")

    val filterCount = allFilters().size
    val joinCount   = allJoins().size

    // Find the aggregate even if it's wrapped by HAVING/orderBy/limit filters.
    def findAggregate(op: SemanticOp): Option[SemanticAggregateOp] = op match {
      case a: SemanticAggregateOp => Some(a)
      case SemanticFilterOp(src, _)       => findAggregate(src)
      case SemanticOrderByOp(src, _)      => findAggregate(src)
      case SemanticLimitOp(src, _)        => findAggregate(src)
      case _                             => None
    }
    findAggregate(st.root) match {
      case Some(a) =>
        val keys = if (a.keys.isEmpty) "(all rows)" else a.keys.mkString(", ")
        val meas = a.measureNames.mkString(", ")
        sb.append(s"  group by: $keys\n")
        sb.append(s"  compute:  $meas\n")
      case None =>
        st.root match {
          case t: SemanticTableOp =>
            sb.append(s"  type:     scan (no aggregation)\n")
          case _ =>
            sb.append(s"  type:     derived table (subquery or view)\n")
        }
    }

    if (filterCount > 0) sb.append(s"  filters: $filterCount applied\n")
    if (joinCount   > 0) sb.append(s"  joins:   $joinCount\n")
    sb.toString
  }

  private def renderRouting(): String = {
    val measureNames = allMeasures().map(_._1).toSet
    val filters = allFilters()
    val sb = new StringBuilder
    sb.append(heading("SEMANTIC ROUTING")).append("\n")

    if (filters.isEmpty) {
      sb.append("  (no filters applied)\n")
      return sb.toString
    }

    filters.foreach { case (SemanticFilterOp(_, pred), isHaving) =>
      val label    = if (isHaving) "HAVING \u2192" else "WHERE  \u2192"
      val reason   = if (isHaving) "runs after aggregation (slower)" else "runs before aggregation (fast)"
      val fields   = pred.fields.toSeq.sorted.mkString(", ")
      val (pre, post) = Predicate.splitFilter(pred, measureNames)

      sb.append(s"  $label ${pred.describe}\n")
      sb.append(s"      \u2514\u2500 $reason; touches: $fields\n")

      if (pred.isInstanceOf[Predicate.And] && pre.nonEmpty && post.nonEmpty) {
        sb.append(s"      \u2514\u2500 compound AND split at compile time:\n")
        sb.append(s"         \u2192 WHERE  (pre-agg):  ${pre.map(_.describe).mkString(" AND ")}\n")
        sb.append(s"         \u2192 HAVING (post-agg): ${post.map(_.describe).mkString(" AND ")}\n")
      }
    }
    sb.toString
  }

  private def renderTransitiveDeps(): String = {
    val requested = scala.collection.mutable.LinkedHashSet.empty[String]
    def walk(op: SemanticOp): Unit = op match {
      case a: SemanticAggregateOp => a.measureNames.foreach(requested.add)
      case SemanticFilterOp(src, _)    => walk(src)
      case SemanticOrderByOp(src, keys) =>
        keys.foreach(k => requested.add(SortKey.nameOf(k)))
        walk(src)
      case SemanticLimitOp(src, _)     => walk(src)
      case _: SemanticJoinOp           =>
      case _: SemanticTableOp          =>
    }
    walk(st.root)

    val all        = allMeasures().map(_._1).toSet
    val requested0 = requested.toSet
    val skipped    = all -- requested0
    val unknown    = requested0 -- all

    val sb = new StringBuilder
    sb.append(heading("TRANSITIVE DEPENDENCIES"))
    sb.append("  Measures computed (requested directly or by other calc measures).\n")
    sb.append("  Column pruning means Spark only reads the columns it actually needs.\n")
    sb.append("\n")
    sb.append(s"  Will compute: ${requested.toSeq.sorted.mkString(", ")}\n")
    if (unknown.nonEmpty) {
      sb.append(s"  ⚠ Unknown (typo?): ${unknown.toSeq.sorted.mkString(", ")}\n")
    }
    if (skipped.nonEmpty) {
      sb.append(s"  Skipped (not needed): ${skipped.toSeq.sorted.mkString(", ")}\n")
      sb.append(s"  Spark will not compute these — column pruning skips them\n")
    }
    if (requested0.isEmpty && all.nonEmpty) {
      sb.append(s"  (all ${all.size} measures available but none requested yet)\n")
    }
    sb.toString
  }

  private def renderDimensions(): String = {
    val dims = allDimensions()
    val sb = new StringBuilder
    sb.append(heading(s"DIMENSIONS (${dims.size})"))
    sb.append("  Columns you can group by, filter on, or use in orderBy.\n")
    sb.append("\n")
    if (dims.isEmpty) {
      sb.append("  (none)\n")
      return sb.toString
    }
    dims.foreach { case (name, d) =>
      val flags = scala.collection.mutable.ListBuffer.empty[String]
      if (d.isTimeDimension)        flags += "time"
      if (d.isEntity)               flags += "entity"
      if (d.isEventTimestamp)       flags += "event_ts"
      if (d.smallestTimeGrain.isDefined) flags += s"grain=${d.smallestTimeGrain.get}"
      if (d.metadata.get("pii").contains("true")) flags += "pii"
      val tag  = if (flags.isEmpty) "" else s"  [${flags.mkString(", ")}]"
      val desc = d.description.fold("")(dd => s"  — $dd")
      sb.append(s"  $name$tag$desc\n")
    }
    sb.toString
  }

  private def renderMeasures(): String = {
    val measures = allMeasures()
    val sb = new StringBuilder
    sb.append(heading(s"MEASURES (${measures.size})"))
    sb.append("  Aggregations: base = direct agg; calc = built from other measures.\n")
    sb.append("\n")
    if (measures.isEmpty) {
      sb.append("  (none)\n")
      return sb.toString
    }

    // Probe each measure's expr to detect which other measures it references.
    // Uses a SemanticScope that records refs but never executes — same idea as
    // SemanticOp's ClassificationScope, but DataFrame-free.
    val known = measures.map(_._1).toSet
    measures.foreach { case (name, m) =>
      val probe = new MeasureProbeScope(known - name)
      try { m.expr(probe) } catch { case _: Throwable => /* probe failure => base */ }
      val deps = probe.referenced.toSet
      val kind = if (deps.nonEmpty) "calc" else "base"
      val tag  = s"[$kind]"
      val desc = m.description.fold("")(d => s"  — $d")
      sb.append(s"  $name  $tag$desc\n")
      if (deps.nonEmpty) {
        val sorted = deps.toSeq.sorted.mkString(", ")
        sb.append(s"    pulls in: $sorted\n")
      }
    }
    sb.toString
  }

  private def renderJoins(): String = {
    val joins = allJoins()
    val sb = new StringBuilder
    sb.append(heading(s"JOINS (${joins.size})"))
    if (joins.isEmpty) {
      sb.append("  (none)\n")
      return sb.toString
    }
    joins.foreach { j =>
      val card     = j.cardinality.toString.toUpperCase
      val strategy = j.cardinality match {
        case JoinCardinality.One   => "LEFT JOIN — each row on the right matches at most one on the left"
        case JoinCardinality.Many  => "PRE-AGGREGATE both sides, then JOIN — prevents fan-out explosion"
        case JoinCardinality.Cross => "CROSS JOIN — every row on the left x every row on the right"
      }
      sb.append(s"  $card  $strategy\n")
      if (j.extraDimensions.nonEmpty) {
        sb.append(s"    brings in dimensions: ${j.extraDimensions.keys.toSeq.sorted.mkString(", ")}\n")
      }
      if (j.extraMeasures.nonEmpty) {
        sb.append(s"    brings in measures:  ${j.extraMeasures.keys.toSeq.sorted.mkString(", ")}\n")
      }
      sb.append(s"    join keys visible in SPARK PLAN below\n")
    }
    sb.toString
  }

  private def collectWarnings(): Seq[String] = {
    val out = scala.collection.mutable.ListBuffer.empty[String]
    val allMs = allMeasures().toMap
    // 1. Cycle detection on calc measures
    def isCalcOf(m: Measure, target: String, depth: Int = 0): Boolean = {
      if (depth > 32) return false
      val exprSrc = m.expr.toString
      allMs.values.filter(_ ne m).exists { other =>
        exprSrc.contains(s""""${other.name}""") && (
          other.name == target || isCalcOf(other, target, depth + 1)
        )
      }
    }
    allMs.foreach { case (n, m) =>
      if (isCalcOf(m, n)) out += s"'$n' depends on itself (cycle)"
    }
    // 2. Time dimensions without smallestTimeGrain (might surprise atTimeGrain)
    allDimensions().foreach { case (n, d) =>
      if (d.isTimeDimension && d.smallestTimeGrain.isEmpty)
        out += s"time dimension '$n' has no smallestTimeGrain — atTimeGrain() will raise on any request"
    }
    // 3. AND predicate that mixes dim + measure categories (whole predicate goes to HAVING)
    val measureNames = allMs.keySet
    allFilters().foreach { case (SemanticFilterOp(_, pred), _) =>
      if (pred.isInstanceOf[Predicate.And]) {
        val children = pred.asInstanceOf[Predicate.And].children
        val refsMeasure   = children.exists(p => Predicate.referencesMeasure(p, measureNames))
        val refsDimension = children.exists(p => !Predicate.referencesMeasure(p, measureNames))
        if (refsMeasure && refsDimension) {
          out += s"compound AND mixes dim + measure conditions: ${pred.describe} — whole predicate goes post-agg"
        }
      }
    }
    out.toSeq
  }

  private def renderWarnings(warnings: Seq[String]): String = {
    val sb = new StringBuilder
    sb.append(heading("WARNINGS"))
    warnings.foreach { w => sb.append(s"  ⚠  $w\n") }
    sb.toString
  }

  private def renderSparkPlan(spark: SparkSession): String = {
    val plan = try {
      val df = st.toDataFrame(spark)
      df.queryExecution.explainString(
        org.apache.spark.sql.execution.ExplainMode.fromString("simple")
      )
    } catch {
      case e: Throwable => s"(failed to render Spark plan: ${e.getClass.getSimpleName}: ${e.getMessage})"
    }
    heading("SPARK PLAN (df.explain)") + indent(plan) + "\n"
  }

  /** All table names reachable from the root, in op-tree order (root first, then left-to-right).
    * Used to label the PLAN SUMMARY. Returns an empty list if no [[SemanticTableOp]] is
    * reachable (rare). */
  private def allTableNames: Seq[String] = {
    val acc = scala.collection.mutable.ListBuffer.empty[String]
    def walk(op: SemanticOp): Unit = op match {
      case t: SemanticTableOp => t.name.foreach(acc += _)
      case j: SemanticJoinOp  => walk(j.left); walk(j.right)
      case SemanticAggregateOp(src, _, _) => walk(src)
      case SemanticFilterOp(src, _)       => walk(src)
      case SemanticOrderByOp(src, _)      => walk(src)
      case SemanticLimitOp(src, _)        => walk(src)
    }
    walk(st.root)
    acc.toSeq
  }
}

/** Lightweight probe used by [[SemanticPlanRenderer]] to classify a measure as base
  * vs calc without needing a real DataFrame. Records which known-measure names a
  * lambda references via `t(name)`; returns `lit(0.0)` for everything (never executes).
  * Mirrors the intent of [[SemanticOp.ClassificationScope]] but is self-contained
  * here so the renderer does not depend on a SparkSession. */
private final class MeasureProbeScope(known: Set[String]) extends SemanticScope {
  private[semantica] val referenced = scala.collection.mutable.Set.empty[String]
  override def apply(name: String): Column = {
    if (known.contains(name)) referenced += name
    org.apache.spark.sql.functions.lit(0.0)
  }
}


