package io.semanticdf

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, lit, sum}

import scala.util.DynamicVariable

/** Root of the immutable semantic op tree (DESIGN §4.1).
  *
  * Invariants (DESIGN §4.4): nodes are pure case classes; the compiled DataFrame is
  * never stored in a node; the tree holds no SparkSession; every terminal recompiles
  * against the passed session.
  */
sealed trait SemanticOp extends Serializable with Product {
  def compile(spark: SparkSession): DataFrame
}

object SemanticOp {

  /** Walk to the leaf `SemanticTableOp` (single-table models only).
    *
    * Pre-join row filters ([[SemanticRowFilterOp]]) are transparent wrappers —
    * they apply a row predicate to the source DataFrame but do not change the
    * declared model. Walk through them to reach the underlying leaf. */
  private[semanticdf] def rootModel(op: SemanticOp): Option[SemanticTableOp] = op match {
    case t: SemanticTableOp             => Some(t)
    case j: SemanticJoinOp              => None  // joined — use SemanticJoinOp.mergedModel
    case SemanticAggregateOp(src, _, _) => rootModel(src)
    case SemanticFilterOp(src, _)       => rootModel(src)
    case SemanticRowFilterOp(src, _, _, _, _) => rootModel(src)  // pre-join filter is transparent
    case SemanticOrderByOp(src, _)      => rootModel(src)
    case SemanticLimitOp(src, _)        => rootModel(src)
    case SemanticHintOp(src, _, _)      => rootModel(src)
  }
}

// ---------------------------------------------------------------------------
// Phase 4: Join cardinalities (DESIGN §7)
// ---------------------------------------------------------------------------

/** Join cardinality, mirroring BSL's `join_one` / `join_many` / `join_cross`.
  *
  * - `One`: one-to-one / parent-to-child. Post-agg join is safe (no fan-out).
  * - `Many`: one-to-many / fan-out. Each table is pre-aggregated at its join-key grain
  *   before joining to prevent fact-row multiplication (BSL safe-aggregation, DESIGN §7.1).
  * - `Cross`: Cartesian product.
  */
sealed trait JoinCardinality
object JoinCardinality {
  case object One   extends JoinCardinality
  case object Many  extends JoinCardinality
  case object Cross extends JoinCardinality
}

/** Proxy passed to a join predicate `(l, r) => …`. Each side's `apply("col")`
  * returns a Spark [[Column]] resolved against that side's DataFrame.
  *
  * Not `final` because [[JoinSide.recording]] returns a subclass that overrides
  * `apply` for probe-time capture (see [[JoinKeyProbe]]). */
private[semanticdf] class JoinSide(
    private[semanticdf] val sideName: String,
    private[semanticdf] val df: DataFrame,
    private[semanticdf] val dims: Map[String, Dimension],
    private[semanticdf] val captured: scala.collection.mutable.Map[String, Boolean],
) {
  def apply(name: String): Column = {
    captured.put(name, true)
    if (df.columns.contains(name)) df(name)
    else throw new IllegalArgumentException(
      s"Column '$name' not found on table '$sideName'. " +
        s"Available: ${df.columns.sorted.mkString(", ")}"
    )
  }
}

private[semanticdf] object JoinSide {
  /** Sentinel for [[JoinSide.df]] in probe mode. Never queried because the
    * recording subclass overrides `apply` to skip the df check. Cast from
    * `null` to satisfy the type. */
  private val NullDf: DataFrame = null

  /** Recording-only stub: captures names but never resolves against a DF.
    * Used at [[SemanticJoinOp]] construction to discover join keys without
    * compiling the model. The returned [[Column]] is discarded; only the
    * captured-name side effect matters. */
  def recording(sideName: String, captured: scala.collection.mutable.Map[String, Boolean]): JoinSide =
    new JoinSide(sideName, NullDf, Map.empty, captured) {
      override def apply(name: String): Column = {
        captured.put(name, true)
        import org.apache.spark.sql.functions.lit
        lit(null.asInstanceOf[Any])
      }
    }
}

/** Probe the user-supplied `on` lambda at [[SemanticJoinOp]] construction to
  * capture join-key column names without compiling the model.
  *
  * Used by MCP `describe_model.joins` — needs keys at model-describe time,
  * before any `.toDataFrame(spark)` call. Before this, keys came from
  * [[SemanticJoinOp._grainColsDyn]] which is only populated during `compile`,
  * so any pre-compile introspection saw empty keys.
  *
  * The probe runs the user's predicate against [[JoinSide.recording]] stubs
  * that record names but return a throwaway `lit(null)`. The resulting
  * [[Column]] is discarded.
  *
  * For equi-joins (the common case: `l("x") === r("x")`), the intersection of
  * names captured by both sides is the key set. For asymmetric / custom
  * predicates, the LHS-captured names are used as a best-effort fallback
  * (correctness is still enforced at compile time). */
private[semanticdf] object JoinKeyProbe {
  /** Run an `on` lambda against recording stubs and return the (sorted) capture
    * keys. Discards the predicate's resulting [[Column]]. */
  def captureKeys(on: (JoinSide, JoinSide) => Column): Seq[String] = {
    val lCaptured = scala.collection.mutable.Map.empty[String, Boolean]
    val rCaptured = scala.collection.mutable.Map.empty[String, Boolean]
    val lProbe = JoinSide.recording("left",  lCaptured)
    val rProbe = JoinSide.recording("right", rCaptured)
    try on(lProbe, rProbe)
    catch { case _: Throwable => Seq.empty /* leave keys empty on bad predicate */ }
    val lKeys = lCaptured.keys.toSet
    val rKeys = rCaptured.keys.toSet
    val equi = lKeys intersect rKeys
    if (equi.nonEmpty) equi.toSeq.sorted
    else lKeys.toSeq.sorted  // best-effort fallback for asymmetric predicates
  }
}

// ---------------------------------------------------------------------------
// Leaf op
// ---------------------------------------------------------------------------

/** Leaf: a base DataFrame plus the model's dimensions and measures.
  *
  * `compile` returns the base DataFrame unchanged — aggregation happens in
  * [[SemanticAggregateOp]].
  */
final case class SemanticTableOp(
    table: DataFrame,
    name: Option[String],
    description: Option[String],
    dimensions: Map[String, Dimension] = Map.empty,
    measures: Map[String, Measure] = Map.empty,
) extends SemanticOp {
  override def compile(spark: SparkSession): DataFrame = table
}

// ---------------------------------------------------------------------------
// Phase 4: Join op (DESIGN §7)
// ---------------------------------------------------------------------------

/** A joined semantic model (Phase 4). Holds the op tree (`left` ⊕ `right`) and metadata
  * merged from both sides.
  *
  * Compile strategy:
  * - `Cross`: straightforward `crossJoin`.
  * - `One`: standard left outer join; safe to aggregate post-join (no fan-out).
  * - `Many`: fan-out-aware. Each source table's measures are pre-aggregated at the
  *   join-key grain before joining, preventing fact-row multiplication (BSL safe-
  *   aggregation, DESIGN §7.1).
  *
  * Column naming: Spark preserves left key columns in left-outer joins. When both
  * sides use the same key column name (e.g. `customer_id`), the joined DataFrame has
  * a single `customer_id` column (from the left). Measure columns are prefixed with
  * their table name to avoid collisions (e.g. `orders_total_amount`, `customers_customer_count`).
  */
final case class SemanticJoinOp(
    left: SemanticOp,
    right: SemanticOp,
    on: (JoinSide, JoinSide) => Column,
    cardinality: JoinCardinality,
    leftRoot: SemanticTableOp,
    rightRoot: SemanticTableOp,
    extraDimensions: Map[String, Dimension] = Map.empty,
    extraMeasures: Map[String, Measure] = Map.empty,
) extends SemanticOp {

  /** The merged model combining dimensions and measures from both sides,
    * plus any dimensions/measures added via `withDimensions`/`withMeasures`.
    *
    * Join-key columns (shared between both sides by necessity) are allowed to collide —
    * the LEFT side's definition takes precedence. Non-key collisions also take left
    * precedence rather than throwing; genuine ambiguity surfaces at Spark runtime. */
  private[semanticdf] lazy val mergedModel: MergedSemanticModel = {
    val ld = leftRoot.dimensions; val rd = rightRoot.dimensions
    val lm = leftRoot.measures;   val rm = rightRoot.measures
    // Left precedence: rd first, then ld overrides for shared names (join keys etc.)
    MergedSemanticModel(
      dimensions = rd ++ ld ++ extraDimensions,
      measures   = rm ++ lm ++ extraMeasures,
    )
  }

  /** Per-thread storage for the grain (join key) column names captured during
    * `compile`. Replaces a `var _grainCols: Seq[String]` field that violated the
    * immutability invariant in DESIGN §4.4 and made concurrent compiles racy.
    *
    * `DynamicVariable` (Scala's typed wrapper around `ThreadLocal`) gives each
    * thread its own slot. A compile on thread A leaves thread B's view untouched,
    * which is the right semantics: a thread that didn't compile this op can't
    * observe its keys. */
  private val _grainColsDyn = new DynamicVariable[Seq[String]](Seq.empty)

  /** Join keys captured at construction time by probing the user's `on` lambda
    * against [[JoinSide.recording]] stubs. Always populated; the typical
    * equi-join case (`l("x") === r("x")`) puts `Seq("x")` here at construction
    * without compiling the model. Pre-compile `grainCols` reads from this; a
    * successful compile may overwrite the dynamic-variable slot with the
    * post-compile observation (which can differ in pathological cases, but
    * for equi-joins it doesn't). */
  private[semanticdf] val _staticGrainCols: Seq[String] =
    JoinKeyProbe.captureKeys(on)

  override def compile(spark: SparkSession): DataFrame = {
    val leftDf  = left.compile(spark)
    val rightDf = right.compile(spark)

    cardinality match {
      case JoinCardinality.Cross =>
        _grainColsDyn.value = Seq.empty
        leftDf.crossJoin(rightDf)

      case JoinCardinality.One | JoinCardinality.Many =>
        val result = compileEquiJoin(leftDf, rightDf)
        result
    }
  }

  /** Grain (join key) column names. Reads from the [[DynamicVariable]] set
    * during `compile` on the calling thread when available; falls back to the
    * eager probe at construction (see [[_staticGrainCols]]) for callers that
    * haven't compiled yet (e.g. MCP `describe_model`). */
  private[semanticdf] def grainCols: Seq[String] = {
    val dyn = _grainColsDyn.value
    if (dyn.nonEmpty) dyn else _staticGrainCols
  }

  /** Scala 2.13 workaround: spread a Seq[Column] as the only varargs arg.
    * Used for `df.groupBy(all: _*)` and `df.select(all: _*)`.
    * @param df    the DataFrame to operate on
    * @param first the first column (always present)
    * @param rest  remaining columns (may be empty)
    */
  private def groupBySpread(df: DataFrame, first: Column, rest: Seq[Column]): org.apache.spark.sql.RelationalGroupedDataset = {
    val all: Seq[Column] = first +: rest
    df.groupBy(all: _*)
  }

  /** Scala 2.13 workaround: spread a Seq[Column] as the only varargs arg for select. */
  private def selectSpread(df: DataFrame, first: Column, rest: Seq[Column]): DataFrame = {
    val all: Seq[Column] = first +: rest
    df.select(all: _*)
  }

  /** Equi-join with fan-out prevention for `Many` cardinality. */
  private def compileEquiJoin(leftDf: DataFrame, rightDf: DataFrame): DataFrame = {

    // --- Extract join key column names from the predicate ---
    val lCaptured = scala.collection.mutable.Map.empty[String, Boolean]
    val rCaptured = scala.collection.mutable.Map.empty[String, Boolean]
    val lSide = new JoinSide("left",  leftDf,  leftRoot.dimensions,  lCaptured)
    val rSide = new JoinSide("right", rightDf, rightRoot.dimensions, rCaptured)
    val sparkPred: Column = on(lSide, rSide)

    val leftKeys  = lCaptured.keys.toSeq.sorted
    val rightKeys = rCaptured.keys.toSeq.sorted

    if (leftKeys.size != rightKeys.size || leftKeys != rightKeys)
      throw new IllegalArgumentException(
        s"Join predicate must compare equal column names on both sides. " +
          s"Left keys: $leftKeys, right keys: $rightKeys"
      )

    // --- Dimension name collision detection (ambiguous-reference guard) ---
    // Both sides may declare dimensions with the same name. Join-key columns are
    // allowed to collide by necessity; other collisions would surface at Spark
    // execution time as `[AMBIGUOUS_REFERENCE] Reference 'x' is ambiguous, could
    // be: ['x', 'x']` — a confusing error. Detect early with a clear message.
    val leftDims  = leftRoot.dimensions.keys.toSet
    val rightDims = rightRoot.dimensions.keys.toSet
    val keySet    = leftKeys.toSet
    val collisions = (leftDims intersect rightDims) -- keySet
    if (collisions.nonEmpty) {
      val sorted = collisions.toSeq.sorted
      val rightName = rightRoot.name.getOrElse("right")
      throw new IllegalArgumentException(
        s"Dimension name collision across joined tables: ${sorted.map(n => s"'$n'").mkString(", ")} " +
          s"exists on both the left and right sides of the join. " +
          s"Reference the right-side copy via its prefixed name " +
          s"(\"$rightName.shared\"), or rename one side to a unique name before joining.")
    }

    // --- Pre-aggregation for Many (fan-out prevention) ---
    // Each side is pre-aggregated at the join-key grain. We pass the MERGED model's
    // dims+measures (which include extras from withDimensions/withMeasures), but
    // preAggregateAtGrain probes each dim/measure against the DataFrame and silently
    // skips any that don't resolve (e.g. `carrier` on the line_items side).
    val allDims    = mergedModel.dimensions
    val allMeasures = mergedModel.measures
    val (leftAgg, rightAgg) = cardinality match {
      case JoinCardinality.Many =>
        // BSL safe-aggregation: aggregate at source grain → join.
        (
          preAggregateAtGrain(leftDf,  allDims, leftKeys,  allMeasures),
          preAggregateAtGrain(rightDf, allDims, rightKeys, allMeasures),
        )
      case JoinCardinality.One =>
        (leftDf, rightDf)
      case JoinCardinality.Cross =>
        throw new IllegalStateException("Cross handled in compile().")
    }

    _grainColsDyn.value = leftKeys
    SemanticLogger.logJoinCompiled(cardinality.toString, leftKeys, cardinality == JoinCardinality.Many)

    // --- Build Spark equi-join condition ---
    val cond: Column = leftKeys.map(k => leftAgg(k) === rightAgg(k)).reduce(_ && _)

    // --- Execute left-outer join ---
    // For Many: pre-agg prevents fact inflation. For One: no fan-out risk.
    val joined = leftAgg.join(rightAgg, cond, "leftouter")

    // --- Dedup join keys ---
    // Spark's expression-join keeps BOTH sides' key columns. When both sides use the
    // same key name (e.g. "customer_id"), the joined DF has two `customer_id` columns.
    // We can't select by bare name (ambiguous) — must use QUALIFIED references
    // (leftAgg("customer_id") vs rightAgg("name")) that carry their source-DF scoping.
    val dupKeySet = leftKeys.toSet
    val leftRefs       = leftAgg.columns.map(c => leftAgg(c))
    val rightOnlyRefs  = rightAgg.columns.filterNot(dupKeySet.contains).map(c => rightAgg(c))
    val keepRefs       = leftRefs ++ rightOnlyRefs
    if (keepRefs.length < joined.columns.length) {
      val first :: rest = keepRefs.toList
      selectSpread(joined, first, rest)
    } else joined
  }

  /** Pre-aggregate a DataFrame at the given grain (Phase 4 fan-out prevention).
    *
    * Measures are aggregated at `grainCols`. ALL dimensions in `dims` are included in
    * the groupBy — not just the join keys — so non-key dimensions (e.g. `carrier` on
    * the orders table) are preserved through the pre-agg. This is safe when non-key
    * dimensions are functionally dependent on the grain (the common star-schema case:
    * one order_id → one carrier, one customer_id).
    *
    * Returns the pre-aggregated DataFrame with all dimensions + aggregated measures. */
  private def preAggregateAtGrain(
      df: DataFrame,
      dims: Map[String, Dimension],
      grainCols: Seq[String],
      measures: Map[String, Measure],
  ): DataFrame = {
    // Probe each dim/measure against the DataFrame and skip any that don't resolve.
    // This handles the merged-model case where a dim (e.g. `carrier`) belongs to the
    // other side and its column doesn't exist in this DataFrame.
    val resolvableDims = dims.values.toSeq.sortBy(_.name).filter { d =>
      try { d.expr(new BaseScope(df)); true }
      catch { case _: SemanticScope.UnknownFieldError => false }
    }
    val resolvableMeasures = measures.values.toSeq.filter { m =>
      try { m.expr(new BaseScope(df)); true }
      catch { case _: SemanticScope.UnknownFieldError => false }
    }

    val allDims: Seq[Dimension] = resolvableDims
    val scope = new BaseScope(df)

    val grainExprs: Seq[Column] = allDims.map(d => d.expr(scope).as(d.name))
    val aggExprs: Seq[Column]   = resolvableMeasures.map(m => m.expr(scope).as(m.name)).toSeq

    if (aggExprs.isEmpty && grainExprs.isEmpty)
      throw new IllegalArgumentException(
        s"No dimensions or measures to aggregate at grain [$grainCols]."
      )

    // Pre-aggregated tables keep their grain columns named as-is. When both sides
    // use the same key name, the join uses equi-join on the same name — Spark
    // outer-join semantics keep only the left key column in the result, so there's
    // a single key column for groupBy to use.
    if (aggExprs.nonEmpty) {
      if (grainExprs.isEmpty)       df.agg(aggExprs.head, aggExprs.tail: _*)
      else                           groupBySpread(df, grainExprs.head, grainExprs.tail).agg(aggExprs.head, aggExprs.tail: _*)
    } else if (grainExprs.nonEmpty) {
      // No measures to aggregate: add a count dummy to complete the groupBy,
      // producing a DataFrame with exactly the grain columns.
      import org.apache.spark.sql.functions.count
      groupBySpread(df, grainExprs.head, grainExprs.tail).agg(count(lit(1)).as("__row_count"))
    } else {
      throw new IllegalArgumentException(
        s"Pre-aggregation at grain [$grainCols]: no dimensions or measures specified."
      )
    }
  }
}


// ---------------------------------------------------------------------------
// Phase 5: Filter op (WHERE / HAVING, DESIGN §6.5)
// ---------------------------------------------------------------------------

/** A filter applied to a semantic source (Phase 5).
  *
  * Wraps a source op and applies a [[Predicate]] when compiled. Used for both:
  * - **pre-aggregation** (WHERE): wraps the source table → filters base rows.
  * - **post-aggregation** (HAVING): wraps a [[SemanticAggregateOp]] → filters
  *   aggregated rows (measure columns exist by then).
  *
  * The same node serves both because predicate compilation is name-based: the
  * predicate resolves field names against whatever DataFrame it's handed.
  */
final case class SemanticFilterOp(
    source: SemanticOp,
    predicate: Predicate,
) extends SemanticOp {
  override def compile(spark: SparkSession): DataFrame = {
    val df = source.compile(spark)
    val scope = new BaseScope(df)
    val clause = source match {
      case _: SemanticAggregateOp => "HAVING"
      case _                      => "WHERE"
    }
    SemanticLogger.logFilterApplied(predicate.describe, clause)
    df.filter(predicate.compile(scope))
  }
}

/** Pre-join row filter declared via YAML `filters:` block.
  *
  * Holds a Spark SQL filter expression (string) plus metadata for OKF / MCP
  * exposure. Compiles to `df.filter(expr)` against the SOURCE DataFrame — the
  * join hasn't run yet at filter time, so joined-side columns are NOT visible.
  * The YamlLoader enforces source-only via `SparkFilterValidator`.
  *
  * Distinct from [[SemanticFilterOp]]: `SemanticFilterOp` carries a structured
  * Predicate for query-time use (`st.where(...)`, `.query(where = ...)`);
  * `SemanticRowFilterOp` carries a Spark SQL string for YAML-declared model
  * hygiene. Both reduce to a row-level `df.filter(...)` at compile time.
  */
final case class SemanticRowFilterOp(
    source: SemanticOp,
    name: String,
    description: Option[String],
    expr: String,
    metadata: Map[String, String],
) extends SemanticOp {
  override def compile(spark: SparkSession): DataFrame = {
    val df = source.compile(spark)
    SemanticLogger.logFilterApplied(s"row-filter($name): $expr", "WHERE")
    df.filter(expr)
  }
}

/** Order the result of a semantic op by one or more [[SortKey]]s (Phase 5 completion).
  *
  * A deferred, composable node — typically chained after `aggregate()`. Sort keys are
  * name-based, resolved against the compiled DataFrame at execution time. */
final case class SemanticOrderByOp(
    source: SemanticOp,
    keys: Seq[SortKey],
) extends SemanticOp {
  override def compile(spark: SparkSession): DataFrame =
    source.compile(spark).orderBy(keys.map(_.toColumn): _*)
}

/** Limit the result of a semantic op to the first `n` rows (Phase 5 completion).
  *
  * Deferred and composable. `offset` is deferred — Spark 3.5 has no Dataset `.offset()`. */
final case class SemanticLimitOp(source: SemanticOp, n: Int) extends SemanticOp {
  override def compile(spark: SparkSession): DataFrame =
    source.compile(spark).limit(n)
}

/** A planner hint applied to the compiled DataFrame (Phase 8: `withHint`).
  *
  * Wraps the source's compiled DataFrame with `df.hint(strategy, params*)`. Common
  * uses: forcing a known-small dimension to broadcast on downstream joins, or
  * setting a partition count for a shuffle-heavy aggregate. Unknown strategies
  * are tolerated by Spark (the hint is added but ignored by Catalyst), so we
  * don't validate the name here.
  */
final case class SemanticHintOp(
    source: SemanticOp,
    strategy: String,
    params: Seq[Any],
) extends SemanticOp {
  override def compile(spark: SparkSession): DataFrame =
    if (params.isEmpty) source.compile(spark).hint(strategy)
    else                 source.compile(spark).hint(strategy, params: _*)
}

/** A merged model assembled from two joined tables. Used by [[SemanticAggregateOp]]
  * so that measure/dimension lookups are resolved from the correct side (via prefix). */
private[semanticdf] final case class MergedSemanticModel(
    dimensions: Map[String, Dimension],
    measures: Map[String, Measure],
)

// ---------------------------------------------------------------------------
// Aggregate op (handles single-table and Phase 4 joined sources)
// ---------------------------------------------------------------------------

/** Group-by + aggregate. Resolves measure expressions from the root model and compiles
  * them in two passes (DESIGN §6.1, updated §7 for joined models):
  *
  *   1. base measures → `groupBy(keys).agg(...)`, each aliased to its measure name;
  *   2. calc measures → one `select` per topological layer.
  *
  * For joined sources (Phase 4), the model is [[SemanticJoinOp.mergedModel]] so that
  * prefixed names (`"orders.total_amount"`) resolve correctly.
  */
/** Per-measure classification produced by [[SemanticAggregateOp.classifyOne]].
  * `deps` = known-measure names the lambda references (non-empty → calc).
  * `totals` = measure names referenced via `t.all(...)` (Phase 3 percent-of-total). */
private case class Classification(deps: Set[String], totals: Set[String])

final case class SemanticAggregateOp(
    source: SemanticOp,
    keys: Seq[String],
    measureNames: Seq[String],
) extends SemanticOp {

  /** Prefix for grand-total columns cross-joined in for percent-of-total (Phase 3). */
  private val TotalColPrefix = "__total__"

  override def compile(spark: SparkSession): DataFrame =
    compileWithBase(spark, source.compile(spark), keys, computeTotals = true)

  /** Core compilation, parameterized so the percent-of-total totals table can be built
    * by re-running the SAME aggregation at zero grain over the SAME already-compiled
    * `base` (no double source-compile). `computeTotals = false` suppresses totals-building
    * to avoid infinite recursion when computing the totals table itself.
    *
    * Phases:
    *   1. base measures → `groupBy(keys).agg(...)`, each aliased to its measure name;
    *   2. (Phase 3) if any calc uses `t.all(...)`, build a 1-row grand-total table at
    *      zero grain and cross-join it (broadcast); expose its columns to calcs;
    *   3. calc measures → one `select` per topological layer; drop totals columns.
    */
  private def compileWithBase(
      spark: SparkSession,
      base: DataFrame,
      groupKeys: Seq[String],
      computeTotals: Boolean,
      measuresToCompute: Seq[String] = measureNames,
  ): DataFrame = {

    // --- Resolve the semantic model (single-table or joined) ---
    val model = resolveModel(source)

    // --- Resolve requested measures ---
    // Measure lookups: declared measures first; if not declared, fall back
    // to a source DataFrame column (typically added via withTransforms).
    // The fall-through case wraps the column in sum() in Pass 1 (see the
    // base.columns check below) — for per-row columns the user wants to
    // aggregate, this is the right default.
    val allMeasures = measuresToCompute.map { n =>
      model.measures.get(n) match {
        case Some(m) => m
        case None if base.columns.contains(n) =>
          // Synthetic measure: identity on the column. The compile
          // pipeline's `if (base.columns.contains(m.name))` branch then
          // wraps this in `sum(col(m.name))`.
          Measure(n, t => col(n))
        case None =>
          throw new IllegalArgumentException(s"Unknown measure '$n'.${closestMatch(n, model.measures.keys).map(c => s" Did you mean: '$c'?").getOrElse("")}")
      }
    }

    if (allMeasures.isEmpty)
      throw new IllegalArgumentException(
        "aggregate() resolved no measures. Pass at least one measure name."
      )

    // --- Auto-pull transitive calc dependencies (Phase 2a) ---
    val allMeasuresClosed = transitiveClosure(allMeasures, model, base)

    // --- Classify into base vs calc ---
    val classifications = allMeasuresClosed.map(m => m.name -> classifyOne(m, base, allMeasuresClosed)).toMap
    val baseMeasures = allMeasuresClosed.filter(m => classifications(m.name).deps.isEmpty)
    val calcMeasures = allMeasuresClosed.filter(m => classifications(m.name).deps.nonEmpty)
    SemanticLogger.logAggregateStart(keys, allMeasuresClosed.map(_.name), measureNames)
    classifications.foreach { case (name, c) =>
      val kind = if (c.deps.isEmpty) "base measure" else "calc measure"
      SemanticLogger.logMeasureClassified(name, kind, c.deps, c.totals)
    }

    // --- Cycle detection (before base-measure check) ---
    val calcDeps: Map[String, Set[String]] =
      calcMeasures.map(m => m.name -> classifications(m.name).deps).toMap
    val layers = topologicalLayers(calcMeasures.map(_.name), calcDeps) // throws on cycle
    if (layers.nonEmpty) SemanticLogger.logCalcLayers(layers)

    if (baseMeasures.isEmpty)
      throw new IllegalArgumentException(
        "aggregate(" + measureNames.map(m => "'" + m + "'").mkString(", ") + "): all requested " +
          "measures are calcs with no base measure at the root of their dependency chain. " +
          "A calc must reference at least one base measure (e.g. sum, count, avg of a column)."
      )

    // --- Pass 1: group-by + aggregate the base measures ---
    val baseScope = new BaseScope(base)
    // Resolve group keys via the dimension's expr when a dimension with that name exists
    // (Phase 6: this is what makes atTimeGrain work — it overrides a dim's expr with a
    // date_trunc). Falls back to the raw column for keys without a declared dimension.
    // Backwards-compatible: existing dims use `t => t("col")`, which resolves identically.
    val modelDims = model.dimensions
    val groupCols: Seq[Column] = groupKeys.map { k =>
      modelDims.get(k).map(d => d.expr(baseScope).as(k)).getOrElse(baseScope(k).as(k))
    }
    // For joined sources (Phase 4), base measures may already be pre-aggregated columns
    // in `base` (from preAggregateAtGrain). Re-aggregating them requires summing the
    // existing column, NOT re-evaluating the original source-column expression (which
    // would fail because source columns like `qty` are gone post-join).
    // Some measures that look base may fail in Pass 1 because they reference a column
    // that exists in the aggregated result but not in the base scope (e.g. t("flight_cont")
    // in a calc measure — the column is resolved as a measure in ClassificationScope but
    // as a missing column in BaseScope). Catch the error; if the failing name is a typo
    // (edit distance ≤ 3 to a base column), move to calcMeasuresFixed so Pass 2's
    // MeasureScope surfaces a "Did you mean?" error with the full column set.
    val (aggColsWorked, calcFromBaseFailure) = {
      val ok  = Seq.newBuilder[Column]
      val bad = Seq.newBuilder[Measure]
      baseMeasures.foreach { m =>
        if (base.columns.contains(m.name)) {
          ok += sum(col(m.name)).as(m.name)
        } else {
          try { ok += m.expr(baseScope).as(m.name) }
          catch { case e: SemanticScope.UnknownFieldError =>
            val n = Option(e.getMessage).flatMap(_.split("'").lift(1)).getOrElse("")
            // Catches column TYPOS that are close to either a base column OR a known measure.
            // Pass 2's MeasureScope will surface a proper "Did you mean?" error.
            val suggestionCandidates = base.columns ++ model.measures.keys
            if (closestMatch(n, suggestionCandidates).isDefined) bad += m
            else throw e
          }
        }
      }
      (ok.result(), bad.result())
    }
    val calcMeasuresFixed = if (calcFromBaseFailure.nonEmpty) calcMeasures ++ calcFromBaseFailure else calcMeasures
    val aggCols = aggColsWorked

    var aggregated: DataFrame =
      if (groupKeys.isEmpty) base.agg(aggCols.head, aggCols.tail: _*)
      else               base.groupBy(groupCols: _*).agg(aggCols.head, aggCols.tail: _*)

    // --- Pass 1.5 (Phase 3): percent-of-total totals cross-join ---
    // Any calc may reference `t.all(measure)`. When it does, build the grand-total row
    // (same aggregation, zero grain) over the SAME base and cross-join it. Spark
    // broadcasts the 1-row side. Calc totals are recomputed via their own formulas, so
    // `all()` is correct for calc measures too, not just additive base measures.
    val referencedTotals: Set[String] =
      calcMeasures.flatMap(m => classifications(m.name).totals).toSet
    val needTotals = computeTotals && referencedTotals.nonEmpty
    val totalsResolver: Option[String => Column] =
      if (needTotals) {
        // Build the grand-total table at zero grain over the SAME (already-filtered)
        // base, computing ONLY the measures referenced via t.all(...). Crucially, we
        // do NOT pass the full measureNames here: percent-of-total calcs (pct_of_total)
        // are excluded — re-evaluating them at zero grain would re-fire t.all(...) with
        // no totals table to resolve against (percent-of-percent is meaningless; BSL
        // excludes AllOf calcs from its totals build too). If a referenced total is
        // itself a calc, its own base deps are pulled in by transitiveClosure and its
        // formula is re-applied at zero grain — giving the correct grand-total value.
        SemanticLogger.logTotalsCrossJoin(referencedTotals)
        val totalsRaw = compileWithBase(
          spark, base, Nil, computeTotals = false, referencedTotals.toSeq)
        val totalsRenamed = totalsRaw.select(
          totalsRaw.columns.map(c => totalsRaw(c).as(TotalColPrefix + c)): _*
        )
        // broadcast() forces Catalyst to ship the 1-row totals side to every
        // executor, eliminating shuffle on the cross-join. Without the hint,
        // Catalyst may auto-broadcast (because the side is 1 row) but isn't
        // guaranteed — particularly under `autoBroadcastJoinThreshold=-1`
        // or when the side gets repartitioned. The explicit hint makes the
        // contract clear: this cross-join is always broadcast.
        aggregated = aggregated.crossJoin(broadcast(totalsRenamed))
        val resolve: String => Column = (name: String) => {
          val tc = TotalColPrefix + name
          if (aggregated.columns.contains(tc)) aggregated(tc)
          else throw new IllegalArgumentException(
            s"t.all('$name') is referenced by a calc, but '$name' is not in the aggregated " +
              "result. Add it to aggregate(...) so its grand total can be computed.")
        }
        Some(resolve)
      } else None

    // --- Pass 2: calc measures one select per topological layer (Phase 2a) ---
    // Recompute calcDeps and layers from calcMeasuresFixed (may include misclassified base calcs)
    val calcDepsFixed: Map[String, Set[String]] =
      calcMeasuresFixed.map(m => m.name -> classifications.getOrElse(m.name, Classification(Set.empty, Set.empty)).deps).toMap
    val layersFixed = topologicalLayers(calcMeasuresFixed.map(_.name), calcDepsFixed)
    if (layersFixed.nonEmpty) SemanticLogger.logCalcLayers(layersFixed)
    if (calcMeasuresFixed.nonEmpty) {
      layersFixed.foreach { layer =>
        val layerMeasures = calcMeasuresFixed.filter(m => layer.contains(m.name))
        val scope = new MeasureScope(aggregated, aggregated.columns.toSet, totalsResolver)
        val derived: Seq[Column] = layerMeasures.map(m => m.expr(scope).as(m.name))
        aggregated = aggregated.select((aggregated.columns.map(col) ++ derived): _*)
      }
    }

    // Drop the synthetic totals columns from the user-visible result.
    if (needTotals) {
      val keep = aggregated.columns.filterNot(_.startsWith(TotalColPrefix))
      aggregated.select(keep.map(col): _*)
    } else aggregated
  }

  /** Resolve the semantic model for the given source.
    *
    * Single-table: creates a MergedSemanticModel from the leaf SemanticTableOp.
    * Joined: uses the SemanticJoinOp's pre-built merged model, which raises an error
    * on bare-name collisions (callers use explicit table-prefixed names). */
  private def resolveModel(src: SemanticOp): MergedSemanticModel = {
    // Walk through transparent wrappers (filter/orderBy/limit/row-filter) before
    // checking for join or single-table roots. Without this, `where(...).groupBy().aggregate()`
    // on a filtered or joined model throws "no root SemanticTableOp or SemanticJoinOp".
    // Pre-join row filters are transparent — they apply a row predicate but do not
    // change the declared model, so the underlying SemanticTableOp is the right root.
    def unwrap(op: SemanticOp): SemanticOp = op match {
      case SemanticFilterOp(s, _)          => unwrap(s)
      case SemanticRowFilterOp(s, _, _, _, _) => unwrap(s)
      case SemanticOrderByOp(s, _)         => unwrap(s)
      case SemanticLimitOp(s, _)           => unwrap(s)
      case other                           => other
    }
    unwrap(src) match {
      case join: SemanticJoinOp =>
        join.mergedModel
      case root: SemanticTableOp =>
        MergedSemanticModel(dimensions = root.dimensions, measures = root.measures)
      case other =>
        throw new IllegalStateException(
          s"SemanticAggregateOp has no root SemanticTableOp or SemanticJoinOp (got ${other.getClass.getSimpleName})")
    }
  }

  // --- Transitive closure (Phase 2a) ---

  /** Transitive closure of requested measures over the calc dependency graph.
    * `baseDf` is the compiled source DataFrame (batch source or joined result). */
  private def transitiveClosure(
      requested: Seq[Measure],
      model: MergedSemanticModel,
      baseDf: DataFrame,
  ): Seq[Measure] = {
    val resolved = scala.collection.mutable.LinkedHashMap.empty[String, Measure]
    val queue    = scala.collection.mutable.Queue.empty[String]
    requested.foreach { m =>
      resolved(m.name) = m; queue.enqueue(m.name)
    }
    while (queue.nonEmpty) {
      val name = queue.dequeue()
      val m    = resolved(name)
      // Exclude the measure's own name from the dependency probe: a same-named column
      // reference is base-column aggregation, not a self-dependency (see classifyOne).
      val probe = new ClassificationScope(baseDf, model.measures.keySet - name)
      try m.expr(probe) catch { case _: SemanticScope.UnknownFieldError => }
      probe.referencedMeasures.foreach { dep =>
        if (!resolved.contains(dep)) {
          val dm = model.measures.getOrElse(dep,
            throw new IllegalArgumentException(
              s"Calc measure '$name' references '$dep', which is not declared in the semantic model. " +
                s"Add Measure(\"$dep\", ...) to .withMeasures(...) before using it in a calc."
            ))
          resolved(dep) = dm; queue.enqueue(dep)
        }
      }
      // Phase 3: a `t.all(dep)` reference also requires `dep` to be aggregated (so its
      // grand total can be computed). Pull it into the closure just like a normal dep.
      probe.referencedTotals.foreach { dep =>
        if (!resolved.contains(dep)) {
          val dm = model.measures.getOrElse(dep,
            throw new IllegalArgumentException(
              s"Calc measure '$name' uses t.all('$dep'), but '$dep' is not declared in .withMeasures(...). " +
                s"Add Measure(\"$dep\", ...) to use it in a percent-of-total."
            ))
          resolved(dep) = dm; queue.enqueue(dep)
        }
      }
    }
    resolved.values.toSeq
  }

  // --- Topological calc layering (Phase 2a) ---

  /** Group measure names into topological layers (Kahn's algorithm). */
  private def topologicalLayers(
      calcNames: Seq[String],
      deps: Map[String, Set[String]],
  ): Seq[Seq[String]] = {
    val calcSet = calcNames.toSet
    val relevant: Map[String, Set[String]] =
      deps.iterator.map { case (k, v) => k -> v.filter(calcSet) }.toMap
    val layers  = Seq.newBuilder[Seq[String]]
    val placed  = scala.collection.mutable.Set.empty[String]
    var remaining = calcNames.toList
    while (remaining.nonEmpty) {
      val ready = remaining.filter(n => relevant(n).forall(placed.contains)).toList
      if (ready.isEmpty)
        throw new IllegalArgumentException(
          s"Calc dependency cycle among: ${remaining.sorted.mkString(", ")}. " +
            s"A calc cannot (transitively) depend on itself.")
      layers += ready
      ready.foreach(placed.add)
      remaining = remaining.filterNot(ready.contains)
    }
    layers.result()
  }

  // --- Classification ---

  /** Classify a single measure: known-measure names it references (deps, non-empty →
    * calc) and measure names it references via `t.all(...)` (totals, Phase 3).
    *
    * The measure's OWN name is excluded from the dependency probe: a measure that
    * references a column sharing its name (the common `Measure("x", t => sum(t("x")))`
    * pattern) is aggregating a base column, not depending on itself. Without this
    * exclusion, that legitimate pattern would be misclassified as a self-dependency
    * → false "calc cycle" error. */
  private def classifyOne(
      m: Measure,
      baseDf: DataFrame,
      allMeasures: Seq[Measure],
  ): Classification = {
    val known = allMeasures.map(_.name).toSet - m.name
    val probe = new ClassificationScope(baseDf, known)
    try {
      m.expr(probe)
      Classification(probe.referencedMeasures.toSet, probe.referencedTotals.toSet)
    } catch {
      case _: SemanticScope.UnknownFieldError => Classification(Set.empty, Set.empty)
    }
  }
}

// ---------------------------------------------------------------------------
// Classification scope (Phase 2a)
// ---------------------------------------------------------------------------

/** Classification-only scope: records which known-measure names a lambda touches.
  * Base columns resolve to the real column; known measures resolve to a placeholder
  * and are recorded. Never executed. Powers dependency discovery.
  *
  * Also records `t.all(name)` references (Phase 3) so the planner knows which grand
  * totals to compute and cross-join. */
private[semanticdf] final class ClassificationScope(
    df: DataFrame,
    knownMeasures: scala.collection.Set[String],
) extends SemanticScope {
  private[semanticdf] val referencedMeasures = scala.collection.mutable.Set.empty[String]
  private[semanticdf] val referencedTotals = scala.collection.mutable.Set.empty[String]

  override def apply(name: String): Column =
    // Check known measures FIRST: when a name is both a column (e.g. a pre-aggregated
    // measure from a join) and a known measure, record it as a measure dependency so
    // calc classification works correctly.
    if (knownMeasures.contains(name)) {
      referencedMeasures += name
      lit(0.0)
    } else if (df.columns.contains(name)) df(name)
    else throw new SemanticScope.UnknownFieldError(name, df.columns.toSet ++ knownMeasures)

  /** Record a percent-of-total reference; never executed. */
  override def all(name: String): Column = {
    referencedTotals += name
    lit(0.0)
  }
}
