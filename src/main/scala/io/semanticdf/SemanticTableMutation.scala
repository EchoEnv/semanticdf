package io.semanticdf
import io.semanticdf.predicate._

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Mutation methods of [[SemanticTable]] — `withDimensions`, `withMeasures`,
  * `withTransforms`, and the four join entry points (`join_one`, `join_on`,
  * `join_many`, `join_many_on`, `join_cross`).
  *
  * == Why a trait ==
  *
  * Mutation is the largest concern in `SemanticTable` (~660 lines) and
  * it's tightly cohesive: every method here builds a new
  * `SemanticTable` by wrapping the current root in a new op (`SemanticTableOp`,
  * `SemanticTransformsOp`, `SemanticJoinOp`, etc.). Pulling them out gives
  * mutation its own file with one consistent concern: "extend the model
  * definition."
  *
  * == What lives here ==
  *
  *   - `withDimensions` (with the derived-time-dim materialization)
  *   - `withMeasures` (varargs + typed `[F]` overload)
  *   - `withMeasures0` (private; the funnel all `withMeasures` paths
  *     go through)
  *   - `withTransforms`
  *   - `join_one`, `join_on` (2 overloads), `join_oneWithKeys`
  *     (private helper)
  *   - `join_many`, `join_many_on` (2 overloads), `join_manyWithKeys`
  *     (private helper)
  *   - `join_cross`
  *
  * == Cross-trait dependencies ==
  *
  *   - `requireRoot` lives in `SemanticTableCore` (private[semanticdf]);
  *     the join entry points call it via `self.requireRoot(...)` and
  *     `other.requireRoot(...)`.
  *   - `withDimensions` and `withMeasures` recurse on `new
  *     SemanticTable(src, ...).withDimensions(...)` — the recursion
  *     goes through `self` (the self-type), so it's a method call on
  *     the same class, no new dependency.
  *   - The join builders call `self.root`, `other.root` (the op trees)
  *     — instance fields on the class, accessible from any trait
  *     mixed in.
  *
  * == Doc bug carried forward ==
  *
  * The `withDimensions` docstring at line 415 in the original file
  * was a copy-paste of the `withRowFilter` docstring and says "Attach
  * a pre-join row filter" instead of describing the actual method.
  * This is a pre-existing bug from before the trait split; per
  * karpathy-guidelines (minimum code, surgical changes), it is
  * preserved as-is and will be fixed in a follow-up PR if the user
  * wants.
  *
  * == Public API ==
  *
  * 100% unchanged. The trait is `private[semanticdf]` and is mixed
  * in via `extends SemanticTableMutation`. Consumers see the
  * same `SemanticTable` class.
  */
private[semanticdf] trait SemanticTableMutation { self: SemanticTable =>

  // -------------------------------------------------------------------------
  // Dimension / measure / transform extensions
  // -------------------------------------------------------------------------

  /** Attach a pre-join row filter declared via the YAML `filters:` block.
    *
    * Compiles to `df.filter(expr)` against the current root at execution time.
    * The expression's column references must resolve against THIS model's
    * source table (validation is performed by the YamlLoader before this is called).
    *
    * Returns a NEW SemanticTable (immutability preserved). Filters accumulate
    * in the order they are added — each call wraps the previous root in a new
    * [[SemanticRowFilterOp]].
    *
    * Distinct from [[where]]: `where` accepts a structured `Predicate` for
    * query-time use; `withRowFilter` accepts a Spark SQL string for model-level
    * hygiene declared in YAML.
    */
  def withDimensions(dims: Dimension*): SemanticTable = {
    // Materialize derived-time dims (siblings of any base time-dim with `derived` non-empty).
    // Fails loud on:
    //   - a name in `derived` that's not in {"year", "month", "day"}
    //   - collisions across declared + derived names
    //
    // (Previously this block also validated `derived`-on-non-time-dim. That
    // check is now in the `Dimension` primary constructor (see Model.scala),
    // which fires at `new Dimension(...)` time. The duplicate here was
    // unreachable post-PR-#290; removed in the post-audit cleanup.)
    val materialized: Seq[Dimension] = dims.flatMap { d =>
      if (d.derived.isEmpty) Seq(d)
      else {
        val siblings: Seq[Dimension] = d.derived.map { part =>
          val partCol: Column = part match {
            case "year"  => year(col(d.name))
            case "month" => month(col(d.name))
            case "day"   => dayofmonth(col(d.name))
            case other =>
              throw new IllegalArgumentException(
                s"dimension '${d.name}' declares unsupported derived part '$other'. v0.2 supports: year, month, day.")
          }
          new Dimension(
            name        = part,
            expr        = (_: SemanticScope) => partCol,
            description = d.description.map(_ + s" (derived from '${d.name}')"),
          )
        }
        d +: siblings
      }
    }
    val allNames = materialized.map(_.name)
    val collisions = allNames.groupBy(identity).collect { case (n, xs) if xs.size > 1 => n }.toList
    if (collisions.nonEmpty)
      throw new IllegalArgumentException(
        s"dimension names collide across declared + derived dims: ${collisions.mkString(", ")}")
    val extra = materialized.map(d => d.name -> d).toMap
    root match {
      case t: SemanticTableOp =>
        new SemanticTable(t.copy(dimensions = t.dimensions ++ extra), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Streaming source: dims attach to the streaming model.
      case s: SemanticStreamingTableOp =>
        new SemanticTable(s.copy(dimensions = s.dimensions ++ extra), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      case j: SemanticJoinOp =>
        // Pass extra dimensions so mergedModel includes them. Preserve
        // the key arrays + SQL fallback so the joined manifest
        // round-trip stays correct after adding extra dims.
        val updatedJoin = SemanticJoinOp(
          left   = j.left,
          right  = j.right,
          on     = j.on,
          cardinality = j.cardinality,
          leftRoot  = j.leftRoot,
          rightRoot = j.rightRoot,
          extraDimensions = j.extraDimensions ++ extra,
          extraMeasures   = j.extraMeasures,
          leftSide  = j.leftSide,
          rightSide = j.rightSide,
          leftKeys = j.leftKeys,
          rightKeys = j.rightKeys,
          onExprString = j.onExprString,
          // Path C: prefix fields (recipe §3.6, caveat §1.3).
          leftPrefix = j.leftPrefix,
          rightPrefix = j.rightPrefix,
          // Preserve the structured predicate AST across the
          // extra-* rewrite so the joined-manifest round-trip
          // keeps it it.
          predicateAst = j.predicateAst,
        )
        new SemanticTable(updatedJoin, postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Passthrough ops: recurse to the underlying table/join, then re-wrap.
      // Lets a user (or query()) chain withDimensions after where()/orderBy()/limit().
      case SemanticFilterOp(src, pred) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withDimensions(dims: _*)
        new SemanticTable(SemanticFilterOp(inner.root, pred), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticRowFilterOp(src, name, desc, expr, meta) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withDimensions(dims: _*)
        new SemanticTable(SemanticRowFilterOp(inner.root, name, desc, expr, meta), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticOrderByOp(src, keys) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withDimensions(dims: _*)
        new SemanticTable(SemanticOrderByOp(inner.root, keys), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticLimitOp(src, n) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withDimensions(dims: _*)
        new SemanticTable(SemanticLimitOp(inner.root, n), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Hint is a Spark planner wrapper; recurse and re-wrap with the same hint.
      case SemanticHintOp(src, strategy, params) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withDimensions(dims: _*)
        new SemanticTable(SemanticHintOp(inner.root, strategy, params), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Transforms are applied at compile time; dims should attach to the underlying
      // model so they're visible to the join/table op. Recurse and re-wrap.
      case SemanticTransformsOp(src, transforms) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withDimensions(dims: _*)
        new SemanticTable(SemanticTransformsOp(inner.root, transforms), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      case _ =>
        throw new IllegalStateException(
          s"withDimensions: unexpected root type ${root.getClass.getSimpleName}"
        )
    }
  }

  /** Extend the model with measures. Handles single-table and joined roots.
    * Returns a new [[SemanticTable]] (immutable). */
  def withMeasures(measures: Measure*): SemanticTable = withMeasures0(measures)

  /** Typed-overload of [[withMeasures(measures:Measure*)*]] — accepts a single
    * [[SemanticMeasure]] witness whose `.name` becomes the measure name. The
    * expr still has signature `SemanticScope => Column`, so window functions
    * (`row_number().over(...)`, `lag(...)`, etc.) work the same as in the
    * string-based form.
    *
    * Multi-measure definitions are still string-based:
    * {{{
    *   flights.withMeasures(rank, t => row_number().over(Window.partitionBy(t("carrier"))...))
    *   flights.withMeasures(Measure("rank", expr), Measure("lag_pax", expr))   // multi-arity string
    * }}}
    *
    * Compile-time guarantee: passing a `SemanticDimension[F]` is a compile
    * error — `SemanticDimension` is not a subtype of `SemanticMeasure`, so
    * this overload is not applicable and the varargs overload rejects it too
    * (a `SemanticDimension` is not a `Measure`). The measure's name is read
    * from the typed witness, not a string — a typo in
    * `SemanticMeasure.of[RankWithinCarrier]("rank_winthin_carier")` would
    * still surface at runtime when the model loads, but downstream
    * `aggregateMeasures(rank)` / `Compare.Le(rank, 5)` are type-checked
    * against the same ref.
    *
    * The first parameter is the typeclass instance itself (not a `FieldRef`),
    * so `SemanticMeasure[F]` matches by subtyping in Scala's phase-1 overload
    * resolution — no implicit conversion is needed, and this overload is
    * picked over the varargs form even from cross-package consumer code. */
  def withMeasures[F](
      measure: SemanticMeasure[F],
      expr: SemanticScope => Column,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): SemanticTable = {
    val m = Measure(name = measure.name, expr = expr, description = description, metadata = metadata)
    withMeasures0(Seq(m))
  }

  /** Internal helper that all `withMeasures` paths funnel through. */
  private def withMeasures0(measures: Seq[Measure]): SemanticTable = {
    val extra = measures.map(m => m.name -> m).toMap
    root match {
      case t: SemanticTableOp =>
        new SemanticTable(t.copy(measures = t.measures ++ extra), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Streaming source: measures attach to the streaming model.
      case s: SemanticStreamingTableOp =>
        new SemanticTable(s.copy(measures = s.measures ++ extra), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

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
          leftSide  = j.leftSide,
          rightSide = j.rightSide,
          leftKeys = j.leftKeys,
          rightKeys = j.rightKeys,
          onExprString = j.onExprString,
          // Path C: prefix fields (recipe §3.6, caveat §1.3).
          leftPrefix = j.leftPrefix,
          rightPrefix = j.rightPrefix,
          // Preserve the structured predicate AST across the
          // extra-* rewrite so the joined-manifest round-trip
          // keeps it it.
          predicateAst = j.predicateAst,
        )
        new SemanticTable(updatedJoin, postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Passthrough ops: recurse to the underlying table/join, then re-wrap.
      case SemanticFilterOp(src, pred) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withMeasures(measures: _*)
        new SemanticTable(SemanticFilterOp(inner.root, pred), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticRowFilterOp(src, name, desc, expr, meta) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withMeasures(measures: _*)
        new SemanticTable(SemanticRowFilterOp(inner.root, name, desc, expr, meta), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticOrderByOp(src, keys) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withMeasures(measures: _*)
        new SemanticTable(SemanticOrderByOp(inner.root, keys), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticLimitOp(src, n) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withMeasures(measures: _*)
        new SemanticTable(SemanticLimitOp(inner.root, n), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Hint is a Spark planner wrapper; recurse and re-wrap with the same hint.
      case SemanticHintOp(src, strategy, params) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withMeasures(measures: _*)
        new SemanticTable(SemanticHintOp(inner.root, strategy, params), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Transforms are applied at compile time; measures should attach to the underlying
      // model so they're visible to the join/table op. Recurse and re-wrap.
      case SemanticTransformsOp(src, transforms) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withMeasures(measures: _*)
        new SemanticTable(SemanticTransformsOp(inner.root, transforms), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      case _ =>
        throw new IllegalStateException(
          s"withMeasures: unexpected root type ${root.getClass.getSimpleName}"
        )
    }
  }

  /** Extend the model with per-row transforms applied to the source data at load
    * time. Transforms correspond to dbt's staging models / LookML's
    * `derived_table` — per-row logic (`datediff`, `case when ...`, window
    * functions) that doesn't fit the `agg()` aggregate context.
    *
    * ==Lazy contract==
    *
    * Transforms are NOT applied when this method is called — they are wrapped
    * in a [[SemanticTransformsOp]] and applied lazily when the consumer calls
    * [[toDataFrame]] (or any other terminal that compiles the op tree). This
    * preserves the lazy compile contract (DESIGN §4.4) — `toDataFrame(spark)` is
    * the only place where Spark actually runs. Other passthrough ops
    * ([[where]], [[orderBy]], [[limit]], etc.) follow the same pattern.
    *
    * ==Why a dedicated op (not eager `withColumn` in this method)==
    *
    * Before this refactor, `withTransforms` on a join model called
    * `j.compile(SparkSession.active)` eagerly to get the joined DataFrame, then
    * applied `withColumn` against it. That broke the lazy contract in two ways:
    *
    *   1. `SparkSession.active` is a side effect — it auto-creates a default
    *      session if none is set. A consumer building a SemanticTable in a
    *      context without a session (config loading, `validate()` calls,
    *      catalog accessors) would silently get a Spark session.
    *   2. The join was forced to build its logical plan at op-tree construction
    *      time. Every other op in the tree is a passthrough that defers
    *      compilation to `toDataFrame`.
    *
    * With [[SemanticTransformsOp]], transforms are applied at the terminal,
    * consistent with the rest of the tree.
    *
    * ==Transform outputs are NOT in the catalog==
    *
    * The output of a transform (e.g. `Transform("los_days", t => datediff(...))`)
    * is a DataFrame column, NOT a catalog dimension or measure. Users reference
    * transform outputs by string in subsequent measure/dimension expressions.
    * Transform outputs cannot be referenced via typed refs (`SemanticDimension` /
    * `SemanticMeasure`) because they aren't declared anywhere — adding them to
    * the catalog is a separate, additive feature.
    *
    * ==Chaining==
    *
    * Calling `withTransforms` multiple times composes all the transforms into
    * a single [[SemanticTransformsOp]] layer, applied in declaration order at
    * `toDataFrame(spark)` time. The earlier transforms are NOT replaced — they
    * compose with the new ones. This is the same `withColumn`-chain semantics
    * you'd get in plain Spark, just deferred.
    *
    * {{{
    *   st
    *     .withTransforms(Transform("a", t => t("v") + 1))   // applied first
    *     .withTransforms(Transform("b", t => t("a") * 2))   // applied second, sees `a`
    * }}}
    *
    * If transform B references a column added by transform A, declare A first
    * (the composition preserves declaration order).
    *
    * @example
    * {{{
    * val orders = ...
    *   .withTransforms(
    *     Transform("los_days",
    *       t => datediff(t("shipped_at"), t("order_date"))))
    *   .withMeasures(Measure("avg_los",
    *     t => sum(t("los_days")) / count(lit(1))))
    * }}}
    */
  def withTransforms(transforms: Transform*): SemanticTable = {
    if (transforms.isEmpty) return this
    root match {
      case t: SemanticTableOp =>
        // Single-table models: wrap in a SemanticTransformsOp. The transforms
        // are applied at toDataFrame() time, not now. This matches the lazy
        // pattern used for joins below.
        new SemanticTable(
          SemanticTransformsOp(t, transforms),
          postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      case j: SemanticJoinOp =>
        // Joined models: wrap in a SemanticTransformsOp. CRUCIALLY, we do NOT
        // call j.compile(...) here — that would force the join to build its
        // logical plan now and would trigger SparkSession.active (the
        // side effect we're fixing). The join is compiled at toDataFrame()
        // time, and the transforms are applied then too.
        new SemanticTable(
          SemanticTransformsOp(j, transforms),
          postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Passthrough ops: recurse to the underlying table/join, then re-wrap.
      case SemanticFilterOp(src, pred) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withTransforms(transforms: _*)
        new SemanticTable(SemanticFilterOp(inner.root, pred), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticRowFilterOp(src, name, desc, expr, meta) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withTransforms(transforms: _*)
        new SemanticTable(SemanticRowFilterOp(inner.root, name, desc, expr, meta), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticOrderByOp(src, keys) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withTransforms(transforms: _*)
        new SemanticTable(SemanticOrderByOp(inner.root, keys), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticLimitOp(src, n) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withTransforms(transforms: _*)
        new SemanticTable(SemanticLimitOp(inner.root, n), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)
      case SemanticHintOp(src, strategy, params) =>
        val inner = new SemanticTable(src, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows).withTransforms(transforms: _*)
        new SemanticTable(SemanticHintOp(inner.root, strategy, params), postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      // Chained transforms: append the new transforms to the existing layer.
      // Do NOT recurse — recursion would re-enter the case below and create a
      // new SemanticTransformsOp with only the new transforms, losing the
      // existing ones. The passthrough ops above (filter/orderBy/etc.) DO
      // recurse because their semantics are "re-apply the transformation to
      // the underlying source", but transforms are cumulative — each call
      // adds to the chain, not replaces it.
      case SemanticTransformsOp(src, existing) =>
        new SemanticTable(
          SemanticTransformsOp(src, existing ++ transforms),
          postAggPredicates, version, sourceTable, status, auditSink, auditRequest, resultCache, maxRows = maxRows)

      case _ =>
        throw new IllegalStateException(
          s"withTransforms: unexpected root type ${root.getClass.getSimpleName}"
        )
    }
  }

  // -------------------------------------------------------------------------
  // Joins
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
  ): SemanticTable = join_oneWithKeys(other, on, Nil, Nil, None)

  /** Typed equi-join entry point: explicit single-column key names.
    * The "core-correct" path — emits the keys directly in the manifest
    * wire shape without needing probe decomposition.
    *
    * @example
    * {{{
    *   orders.join_on(customers, "customer_id" -> "customer_id")
    * }}}
    */
  def join_on(
      other: SemanticTable,
      keys: (String, String),
  ): SemanticTable = {
    val (lk, rk) = keys
    val synthesized = (l: JoinSide, r: JoinSide) => l(lk) === r(rk)
    join_oneWithKeys(other, synthesized, leftKeysIn = Seq(lk), rightKeysIn = Seq(rk), onExprStringIn = None)
  }

  /** Multi-key typed entry point. Keys pair positionally across the two
    * arrays; both must have the same length. The recommended path for
    * 2+ column equi-joins. */
  def join_on(
      other: SemanticTable,
      leftKeys: Seq[String],
      rightKeys: Seq[String],
  ): SemanticTable = {
    require(leftKeys.length == rightKeys.length,
      s"join_on: leftKeys.length (${leftKeys.length}) must equal rightKeys.length (${rightKeys.length}).")
    val synthesized = (l: JoinSide, r: JoinSide) =>
      leftKeys.zip(rightKeys).map { case (lk, rk) => l(lk) === r(rk) }.reduce(_ && _)
    join_oneWithKeys(other, synthesized, leftKeysIn = leftKeys, rightKeysIn = rightKeys, onExprStringIn = None)
  }

  /** Internal: shared body of `join_one` overloads. Calls the probe-based
    * `extractJoinKeys` helper when the caller used the lambda
    * form; the typed entry points pre-populate the keys directly. */
  private[semanticdf] def join_oneWithKeys(
      other: SemanticTable,
      on: (JoinSide, JoinSide) => Column,
      leftKeysIn: Seq[String],
      rightKeysIn: Seq[String],
      onExprStringIn: Option[String],
  ): SemanticTable = {
    // We pass `this.root` and `other.root` (the actual op trees) as the
    // `left`/`right` of the SemanticJoinOp — NOT just the roots. The roots
    // are stored separately. This is important for the streaming terminal:
    // when one side is a SemanticStreamingTableOp, the framework needs the
    // ORIGINAL op (preserving the streaming source info) in `left`/`right`
    // so the streaming-side detection walks correctly.
    //
    // Wrapper ops (Filter, RowFilter, OrderBy, Limit, Hint, Transforms) are
    // transparent for join purposes: we walk through them to find the root
    // model (a SemanticTableOp or SemanticStreamingTableOp).
    val leftOp  = this.root
    val rightOp = other.root
    def rootOf(op: SemanticOp): SemanticOp = op match {
      case t: SemanticTableOp         => t
      case s: SemanticStreamingTableOp => s
      // Pre-join row filters and transforms are transparent — unwrap.
      case rf: SemanticRowFilterOp     => rootOf(rf.source)
      case tr: SemanticTransformsOp    => rootOf(tr.source)
      // Query wrappers (filter/orderBy/limit/hint) and aggregate are not
      // supported as roots for joins — see docs/known-limitations.md.
      case j: SemanticJoinOp =>
        throw new IllegalArgumentException(
          s"join_one: the left side is already a joined table. " +
            "Multi-hop joins are not supported in this version — see docs/known-limitations.md.")
      case a: SemanticAggregateOp =>
        throw new IllegalArgumentException(
          s"join_one: cannot join after aggregate(). Join tables first, then call groupBy().")
      case f: SemanticFilterOp => throw queryWrapperError
      case o: SemanticOrderByOp => throw queryWrapperError
      case l: SemanticLimitOp   => throw queryWrapperError
      case h: SemanticHintOp    => throw queryWrapperError
    }
    // Helper for the query-wrapper rejection — extracted so the four
    // cases above share the same error message.
    def queryWrapperError: IllegalArgumentException =
      new IllegalArgumentException(
        s"join_one: the left/right side is a query wrapper (filter/orderBy/limit/hint). " +
          s"Construct joins from base tables (no query layer above them), then call groupBy() " +
          s"and aggregate() on the joined model.")
    val leftUnderlying  = rootOf(leftOp)
    val rightUnderlying = rootOf(rightOp)
    val leftRoot  = leftUnderlying match {
      case t: SemanticTableOp         => t
      case s: SemanticStreamingTableOp =>
        SemanticTableOp(s.stream, s.name, s.description, s.dimensions, s.measures)
    }
    val rightRoot = rightUnderlying match {
      case t: SemanticTableOp         => t
      case s: SemanticStreamingTableOp =>
        SemanticTableOp(s.stream, s.name, s.description, s.dimensions, s.measures)
    }
    val join = SemanticJoinOp(
      left   = leftOp,
      right  = rightOp,
      on     = on,
      cardinality = JoinCardinality.One,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
      leftSide  = Some(this),
      rightSide = Some(other),
      // When the caller used a typed key entry point, the keys are
      // pre-populated. When the caller used the lambda form, run the
      // probe to extract them at construction time. Either way, the
      // SQL form is also captured so multi-column / non-equi joins
      // still round-trip via the SQL fallback.
      leftKeys = leftKeysIn,
      rightKeys = rightKeysIn,
      onExprString = onExprStringIn,
    )

    // Back-compat: when the lambda path is used and no keys were
    // pre-populated, attempt to decompose the AST. The probe here is
    // cheap (one lambda invocation against recording JoinSide stubs)
    // and idempotent — if it fails we leave the SQL fallback in place.
    if (leftKeysIn.isEmpty && rightKeysIn.isEmpty && onExprStringIn.isEmpty) {
      val (lk, rk, sql) = join.extractJoinKeys()
      if (lk.nonEmpty || rk.nonEmpty || sql.isDefined) {
        // v0.1.13: also extract the structured predicate AST. Only
        // populated when keys alone don't capture the structure
        // (non-equi / OR) so the canonical equi-join case keeps
        // zero AST overhead. The probe reuses the same recording
        // stubs so the additional cost is one extra `Column` build
        // (already discarded).
        val ast = join.extractPredicateAst()
        return new SemanticTable(
          join.copy(leftKeys = lk, rightKeys = rk, onExprString = sql, predicateAst = ast),
          postAggPredicates = Nil,
          version = 0,
          sourceTable = None,
          status = ModelStatus.Published,
          auditSink = this.auditSink,
          auditRequest = this.auditRequest,
          resultCache,
          maxRows = maxRows)
      }
    }
    new SemanticTable(join, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows)
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
  ): SemanticTable = join_manyWithKeys(other, on, Nil, Nil, None)

  /** Typed one-to-many join with explicit single-column keys. See
    * [[join_on]] for the Single-key variant of the one-to-one join.
    *
    * Same construction-time decomposition applies: the typed entry
    * populates `SemanticJoinOp.leftKeys` / `rightKeys` directly without
    * needing the lambda-decomposition probe. */
  def join_many_on(
      other: SemanticTable,
      keys: (String, String),
  ): SemanticTable = {
    val (lk, rk) = keys
    val synthesized = (l: JoinSide, r: JoinSide) => l(lk) === r(rk)
    join_manyWithKeys(other, synthesized, leftKeysIn = Seq(lk), rightKeysIn = Seq(rk), onExprStringIn = None)
  }

  /** Multi-key typed entry point for many-side joins. */
  def join_many_on(
      other: SemanticTable,
      leftKeys: Seq[String],
      rightKeys: Seq[String],
  ): SemanticTable = {
    require(leftKeys.length == rightKeys.length,
      s"join_many_on: leftKeys.length (${leftKeys.length}) must equal rightKeys.length (${rightKeys.length}).")
    val synthesized = (l: JoinSide, r: JoinSide) =>
      leftKeys.zip(rightKeys).map { case (lk, rk) => l(lk) === r(rk) }.reduce(_ && _)
    join_manyWithKeys(other, synthesized, leftKeysIn = leftKeys, rightKeysIn = rightKeys, onExprStringIn = None)
  }

  /** Internal shared body of `join_many` overloads. See [[join_oneWithKeys]]
    * for the same logic on `join_one`. */
  private[semanticdf] def join_manyWithKeys(
      other: SemanticTable,
      on: (JoinSide, JoinSide) => Column,
      leftKeysIn: Seq[String],
      rightKeysIn: Seq[String],
      onExprStringIn: Option[String],
  ): SemanticTable = {
    val leftRoot  = self.requireRoot("join_many (left)")
    val rightRoot = other.requireRoot("join_many (right)")
    val join = SemanticJoinOp(
      left   = leftRoot,
      right  = rightRoot,
      on     = on,
      cardinality = JoinCardinality.Many,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
      leftSide  = Some(this),
      rightSide = Some(other),
      leftKeys = leftKeysIn,
      rightKeys = rightKeysIn,
      onExprString = onExprStringIn,
    )
    // Lambda-path decomposition: same trade-offs as in [[join_oneWithKeys]].
    if (leftKeysIn.isEmpty && rightKeysIn.isEmpty && onExprStringIn.isEmpty) {
      val (lk, rk, sql) = join.extractJoinKeys()
      if (lk.nonEmpty || rk.nonEmpty || sql.isDefined) {
        // v0.1.13: same AST extraction as the one-side path. See
        // join_oneWithKeys for the rationale.
        val ast = join.extractPredicateAst()
        return new SemanticTable(
          join.copy(leftKeys = lk, rightKeys = rk, onExprString = sql, predicateAst = ast),
          postAggPredicates = Nil,
          version = 0,
          sourceTable = None,
          status = ModelStatus.Published,
          auditSink = this.auditSink,
          auditRequest = this.auditRequest,
          resultCache,
          maxRows = maxRows)
      }
    }
    new SemanticTable(join, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows)
  }

  /** Cross join (Cartesian product) with another semantic table (`join_cross`).
    *
    * Every row in `this` is paired with every row in `other`. Use with caution —
    * row counts multiply.
    */
  def join_cross(other: SemanticTable): SemanticTable = {
    val leftRoot  = self.requireRoot("join_cross (left)")
    val rightRoot = other.requireRoot("join_cross (right)")
    val join = SemanticJoinOp(
      left   = leftRoot,
      right  = rightRoot,
      on     = (_, _) => throw new IllegalStateException("Cross join has no predicate."),
      cardinality = JoinCardinality.Cross,
      leftRoot  = leftRoot,
      rightRoot = rightRoot,
      leftSide  = Some(this),
      rightSide = Some(other),
    )
    new SemanticTable(join, auditSink = this.auditSink, auditRequest = this.auditRequest, resultCache = this.resultCache, maxRows = maxRows)
  }
}
