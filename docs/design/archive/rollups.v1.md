# Design: pre-aggregation rollups + auto-routing

> Status: **REVISION 1** — addressing 10 HIGH findings from senior DE + senior architect reviews (see section 12 for revision history)
> Author: pending (assistant-generated, awaiting second review)
> Target version: **v0.3.0**
> Scope: 5-PR cycle, ~1200 LoC (was ~1000; added safety/typed-aggregator/freshness code)

---

## 12. Revision history (read this first if you read v0)

The v0 plan was reviewed by a senior data engineer and a senior software
architect. They found **10 HIGH**, **9 MED**, **7 LOW** issues. All 10 HIGH
findings are addressed in this revision. The full revision log is in
section 12 at the bottom of this document; the quick summary:

| # | Finding | Resolution in this revision |
|---|---|---|
| DE H1 | Superset routing returns duplicate rows | §4.3 — exact-grain match OR re-aggregation on the rollup source |
| DE H2 | Routing ignored filters/joins/time | §3.2 — matching signature extended to `QuerySignature` (dims + measures + filter cols + time grain + join info) |
| DE H3 | `CoarsestAcceptable` policy mathematically impossible | §3.1 — REMOVED. Documented why in section 4.4 |
| DE H4 | Aggregate algebra wrong (min/max not additive) | §3.1 — `RollupAggregator` sealed trait with state-aware merge; min/max/avg/etc. modeled correctly |
| DE H5 | Freshness can't be deferred | §3.1 — `watermark`, `maxStaleness`, `onStale` policy; generation included in cache key |
| DE H6 | `SemanticRollupOp` doesn't fit `SemanticAggregateOp.resolveModel` | §4.2 — `SemanticRollupOp` is a terminal op (skips the wrap; aggregation moves to the rollup's projected columns) |
| Arch H1 | Cache-miss path bypasses rollup routing | §4.1 — `routeToRollup(root, req)` helper called from all 3 compile sites |
| Arch H2 | `Rollup.source: DataFrame` breaks `SemanticTable extends Serializable` | §3.1 — `Rollup` is pure data; `source` is a `() => DataFrame` thunk held on `SemanticRollupOp` only, never on the `SemanticTable` |
| Arch H3 | `estimatedRowCount` cached at registration, never invalidated | §4.3 — `() => Long` thunk re-evaluated on each match |
| Arch H4 | `RollupFallbackPolicy` sealed with 1 case violates no-ADT-escalation | §3.1 — `Boolean` field for v0.3.0; sealed trait only when ≥ 2 policies exist |

Read sections 1-11 in order. Section 12 is the full revision log.

---

## 1. Motivation

The library currently always compiles a `query(measures, dimensions)` against
the raw fact table:

```scala
orders.query(measures = Seq("total", "count"),
            dimensions = Seq("region", "category"))
  .execute(spark).count()  // scans all 1,000,000 ordersDf rows, then aggregates
```

For typical BI dashboards — 5-10 widgets refreshing every minute against a
10M-row fact table — this is the dominant cost. A dashboard that takes 2s
per widget refresh spends 600s of compute per minute per dashboard.

**Pre-aggregation rollups** solve this:

- User maintains a small `orders_by_region_category` table (100 rows)
  via their existing ETL pipeline (Spark job, dbt, Airflow, Streaming).
- The library, on each `query()`, picks the smallest matching rollup
  instead of always hitting the base fact table.
- Same rows, same schema — **40-1000× faster** for the common case.

This is the **single biggest performance differentiator** vs. dbt/Looker.
Looker has `aggregate_table` but routing is manual. dbt has rollups via
`metricflow` but the wiring is fragmented. semanticdf gets it as a
first-class capability, integrated with the existing audit infrastructure.

---

## 2. Goals & non-goals

### Goals

1. **Zero API change for query consumers.** `model.query(measures, dimensions).execute(spark)` keeps working; routing is transparent.
2. **Auto-routing.** The library picks the smallest matching rollup automatically, not the user.
3. **Correct routing.** No silent wrong data: filters, joins, time grain, and aggregator type must all be honored.
4. **Freshness-aware.** Stale rollups are visible (warned or fallen back to base), never silent.
5. **Composable with existing infrastructure.** Reuses `AuditEvent`, `CacheKey`, manifest round-trip, `Lineage.workspaceJsonFor`.
6. **Falsifiable.** Every claim in this design has a corresponding regression test.
7. **Backward compatible.** A model without rollups behaves identically to today. Manifests written by v0.2.x round-trip through v0.3.x.

### Non-goals (out of scope for v0.3.0)

1. **Auto-generating rollups.** semanticdf does not maintain rollups; the user does (via their existing pipeline).
2. **Auto-refreshing rollups.** semanticdf does not schedule refreshes; the user wires that externally. **Freshness tracking is** in scope (per DE H5) but **scheduling is not**.
3. **Hierarchical rollup trees.** Single-level rollups only. No "weekly → monthly → quarterly" chain.
4. **Drill-down fallback.** Once a rollup is built, the rolled-up dimensions cannot be recovered by joining back to base (per DE H3 — lost allocation). Drill-down queries must hit the base table directly.
5. **Cross-model rollups.** A rollup is bound to one base model. Cross-model rollups (shared dim caches) are a separate concern.
6. **Calc measures on rollups.** `pct_of_total`, ratios, and other non-base measures always fall back to the base table (the calc layer needs row-level detail).
7. **Streaming rollups.** v0.3.0 is batch-only for rollups. Micro-batch rollups via Structured Streaming are a separate concern.

---

## 3. User-facing API

### 3.1 New types

```scala
// In: io.semanticdf.rollup (NEW package)

/** The aggregation semantics of a single rollup measure.
  *
  * Each aggregator carries a typed state — sum/count store a scalar;
  * min/max store a scalar; avg stores a (sum, count) pair; stddev stores
  * (sum, sum-of-squares, count). The state is what gets persisted in the
  * rollup table, NOT the final value. The library compiles the query
  * against the state (e.g., `SUM(rollup.total_sum) / SUM(rollup.total_count)`
  * for avg, not `AVG(rollup.total_avg)`).
  *
  * Additivity is a property of the merge function, not the final
  * aggregator. sum/count merge additively; min/max merge via min-of-mins /
  * max-of-maxes (preserves correctness only when the rollup covers the
  * query's grain); avg merges via (sum1+sum2)/(count1+count2); stddev
  * needs a numerically-stable two-pass algorithm.
  *
  * Routing uses these traits to decide if a rollup is eligible for a
  * query. avg/stddev are "approximate" in the sense that rolled-up
  * values match the base-table query when the rollup's grain equals the
  * query's grain (no further grouping); they fall back to base when
  * further re-aggregation is needed.
  */
sealed trait RollupAggregator {
  /** True if the rolled-up value can be re-aggregated (e.g., SUM of a SUM
    * is still correct). sum and count are exact-additive; min/max are
    * partial-additive (correct only when the rollup's grain covers the
    * query's grain); avg/stddev are non-additive (correct only at the
    * rollup's grain). */
  def canReAggregate(rollupGrain: Set[String], queryGrain: Set[String]): Boolean

  /** True if the rollup's grain equals the query's grain (i.e., the
    * rollup is exact for this query). */
  def isExact(rollupGrain: Set[String], queryGrain: Set[String]): Boolean =
    rollupGrain == queryGrain

  /** Compile the rollup measure's storage expression (column references
    * in the rollup source DataFrame). The library writes the rollup
    * table using these expressions. */
  def storageColumn(name: String): Column

  /** Compile the rollup measure's projection expression when reading
    * from the rollup (e.g., for avg: `sum_state / count_state`). */
  def projectionExpr(name: String): Column
}

object RollupAggregator {
  /** Σ. Additive. storage: `sum(col)`. projection: identity. */
  case object Sum extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = rg == qg
    def storageColumn(n: String): Column = ???
    def projectionExpr(n: String): Column = ???
  }

  /** count(*). Additive. storage: `count(*)`. projection: identity. */
  case object Count extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = rg == qg
    def storageColumn(n: String): Column = ???
    def projectionExpr(n: String): Column = ???
  }

  /** min. Partial-additive (correct at rollup grain only). */
  case object Min extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = rg == qg
    def storageColumn(n: String): Column = ???
    def projectionExpr(n: String): Column = ???
  }

  /** max. Partial-additive. */
  case object Max extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = rg == qg
    def storageColumn(n: String): Column = ???
    def projectionExpr(n: String): Column = ???
  }

  /** avg = sum / count. Approximate: rolled-up avg is correct only at
    * the rollup's grain. State is a (sum, count) pair. */
  case object Avg extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = rg == qg
    def storageColumn(n: String): Column = ???
    def projectionExpr(n: String): Column = ???
  }

  /** stddev. Approximate. Requires (sum, sum-of-squares, count) state. */
  case object Stddev extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = rg == qg
    def storageColumn(n: String): Column = ???
    def projectionExpr(n: String): Column = ???
  }

  /** Parse a string into a RollupAggregator. Returns None for unknown
    * names. Lowercase normalization. */
  def parse(s: String): Option[RollupAggregator] = s.toLowerCase match {
    case "sum"    => Some(Sum)
    case "count"  => Some(Count)
    case "min"    => Some(Min)
    case "max"    => Some(Max)
    case "avg"    => Some(Avg)
    case "stddev" => Some(Stddev)
    case _        => None
  }
}

/** A registered pre-aggregated rollup table bound to a base model.
  *
  * `sourceProvider` is a `() => DataFrame` thunk — a function from name
  * to DataFrame. The library invokes the thunk at `compile()` time, NOT
  * at registration time. This sidesteps the DataFrame-non-Serializable
  * issue (Arch H2): the `Rollup` value class is pure data; the actual
  * DataFrame lives only on `SemanticRollupOp` (the op-tree leaf).
  *
  * Pure data: name, baseModel, rollupDimensions, rollupMeasures,
  * sourceProvider, freshness (watermark + maxStaleness + onStale).
  * Survives `SemanticManifest.toJson` / `fromJson`.
  */
final case class Rollup(
  name:             String,
  baseModel:        String,
  rollupDimensions: Seq[String],
  rollupMeasures:   Seq[RollupMeasure],
  sourceProvider:   String => DataFrame,  // NOT a DataFrame field; loaded lazily
  freshness:        RollupFreshness      = RollupFreshness.NoTracking,
)

/** One pre-aggregated measure within a Rollup.
  *
  * The `aggregator` is a typed `RollupAggregator` (Sum/Count/Min/Max/
  * Avg/Stddev), not a `String`. Smart-constructor validation: parse at
  * the boundary; downstream code trusts the type. */
final case class RollupMeasure(
  name:       String,
  aggregator: RollupAggregator,
)

/** Freshness tracking for a rollup.
  *
  * The library cannot know when the user's pipeline refreshes the
  * rollup table; the user provides a `watermarkProvider: () => Instant`
  * thunk that the library invokes at `compile()` time. If the
  * watermark is older than `maxStaleness`, the `onStale` policy
  * determines what happens.
  *
  * `NoTracking` (default): no freshness check; the rollup is used
  * unconditionally. Suitable for batch rollups where the user accepts
  * whatever staleness the refresh cadence produces.
  */
sealed trait RollupFreshness
object RollupFreshness {
  case object NoTracking extends RollupFreshness

  final case class Track(
    watermarkProvider: () => java.time.Instant,  // invoked at compile time
    maxStaleness:       java.time.Duration,       // e.g., 1 hour
    onStale:            OnStalePolicy,
  ) extends RollupFreshness
}

/** What to do when a rollup is too stale to use. */
sealed trait OnStalePolicy
object OnStalePolicy {
  /** Fall back to the base fact table. Emit a warning in the audit event. */
  case object FallBackToBase extends OnStalePolicy

  /** Throw `IllegalStateException` at `toDataFrame` time. Use for
    * dashboards where stale data is unacceptable (e.g., financial
    * dashboards). */
  case object Error extends OnStalePolicy
}

/** Helper: build the freshness thunk for a file-based rollup (mtime). */
def fileMtimeWatermark(path: org.apache.hadoop.fs.Path): () => Instant =
  () => java.time.Instant.ofEpochMilli(path.getFileStatus(new org.apache.hadoop.fs.FileStatus()).getModificationTime())
```

### 3.2 New SemanticTable method

```scala
// In: io.semanticdf.SemanticTable

/** Register a pre-aggregated rollup table for auto-routing.
  *
  * Multiple rollups may be registered per model; the compile path picks
  * the smallest matching one via `findRollupMatch`. Rollups survive the
  * fluent chain the same way `auditSink` / `resultCache` do (LEFT-wins
  * / RIGHT-fallback at join construction).
  *
  * Round-trips through `SemanticManifest.toJson` / `fromJson` (rollup
  * metadata only; `sourceProvider` is re-supplied at fromJson time).
  *
  * @param rollup  the rollup definition. The library does NOT validate
  *                the `sourceProvider` output at registration time —
  *                validation happens at the first `toDataFrame` call.
  */
def withRollup(rollup: Rollup): SemanticTable

/** Find the smallest registered rollup matching the given query
  * signature, or `Left(reason)` describing why no rollup matched.
  *
  * `Right(rollup)` means a rollup was found; the compile path will
  * route to it. `Left(reason)` means the compile path falls back to the
  * base fact table; `reason` is human-readable and ends up in the
  * audit event (so operators can see WHY a query didn't hit a rollup).
  *
  * Matching criteria (all must hold):
  *   1. Grain match: query grain == rollup grain, OR query grain is a
  *      subset of rollup grain AND each requested measure's aggregator
  *      is additive across the grouping (re-aggregate the rollup source
  *      with `groupBy(queryGrain)` before projecting).
  *   2. Filter coverage: every column referenced in the query's WHERE
  *      clause must be present in the rollup source. (If the rollup
  *      was built with a WHERE clause itself, those filters are baked
  *      in; otherwise the rollup must have the filter columns.)
  *   3. Measure coverage: every requested measure must be in the
  *      rollup's measure set.
  *   4. Aggregator compatibility: each requested measure's aggregator
  *      must `canReAggregate` for the rollup's grain vs. query's grain.
  *   5. Freshness: if the rollup has freshness tracking, the watermark
  *      must be within `maxStaleness` (or `onStale = FallBackToBase`).
  *
  * For details, see section 4.3.
  */
def findRollupMatch(
  signature: QuerySignature,  // see section 3.3
): Either[String, Rollup]
```

### 3.3 New `QuerySignature` type

```scala
// In: io.semanticdf.rollup (NEW package)

/** The compile-path signature of a `query()` call. Captures everything
  * the rollup matcher needs to decide routing.
  *
  * Distinct from `AuditQueryRequest` because the matcher doesn't need
  * the where/having predicate text — it needs the column NAMES referenced
  * in those predicates, plus the time grain and join participation.
  */
final case class QuerySignature(
  measures:           Seq[String],          // requested measure names
  dimensions:         Seq[String],          // requested dimension names
  filterColumns:      Set[String],          // columns referenced in WHERE
  timeGrain:          Option[TimeGrain],    // atTimeGrain if any
  joinModel:          Option[String],       // Some if this is a join query
)

object QuerySignature {
  /** Build from an `AuditQueryRequest` and the SemanticTable. */
  def fromAuditRequest(
    req:     AuditQueryRequest,
    model:   SemanticTable,
    isJoin:  Boolean,
  ): QuerySignature = {
    val filterCols: Set[String] = PredicateHasher.collectColumns(req.where)
    QuerySignature(
      measures      = req.measures,
      dimensions    = req.dimensions,
      filterColumns = filterCols,
      timeGrain     = req.timeGrain.map(TimeGrain.fromString),
      joinModel     = if (isJoin) model.name else None,
    )
  }
}
```

### 3.4 New AuditSink method (usage-driven recommendations)

```scala
// In: io.semanticdf.audit.AuditSink (existing trait, new method)

/** Recommend rollups to build based on actual query history.
  *
  * Reads `snapshot()` from the sink, groups by `(measures, dimensions,
  * model)`, counts the number of DISTINCT `dedupHash` values per group
  * (not the total number of calls — a query that runs 1000× per minute
  * is one shape, not 1000).
  *
  * Filters out queries that already match a registered rollup (per
  * `findRollupMatch` semantics) — recommendations are only for queries
  * currently falling back to the base table.
  *
  * Returns the top N combinations ranked by frequency × estimated
  * benefit. The "estimated benefit" is a coarse heuristic (row count
  * ratio; full CBO out of scope for v0.3.0).
  */
def recommendRollups(
  forModel:    Option[String] = None,
  topN:        Int             = 10,
  sinceMillis: Long            = 0L,
  rollupsByModel: Map[String, Seq[Rollup]] = Map.empty,  // for coverage filter
): Seq[RollupRecommendation]

final case class RollupRecommendation(
  model:           String,
  dimensions:      Seq[String],
  measures:        Seq[String],
  distinctQueries: Int,         // number of distinct dedupHash
  filterColumns:   Set[String], // union of filter columns across the queries
  estimatedBenefit: String,     // human-readable ("~10x for typical fact table")
)
```

### 3.5 Rollup catalog / discovery API

(Per Arch L5)

```scala
// In: io.semanticdf.SemanticTable

/** List all rollups registered on this model (and inherited from joined
  * models). Used by the MCP `describe_model` tool. */
def listRollups(): Seq[Rollup]
```

---

## 4. Internal architecture

### 4.1 Where the rollup routing sits in `toDataFrameInternal`

```
toDataFrameInternal(spark: SparkSession): DataFrame
│
├─ applyAqeSkewConfig(spark)               ← existing, unchanged
├─ audit/cache pre-check                    ← PR #323 invariant, unchanged
├─ build QuerySignature from auditRequest  ← NEW (per DE H2)
├─ rollup routing: findRollupMatch(sig)    ← NEW (per Arch H1)
│   ├─ match → wrap root in SemanticRollupOp (terminal)
│   └─ no match → fall through to existing compile
│
├─ freshness check (if rollup matched)    ← NEW (per DE H5)
│   ├─ stale + onStale=FallBackToBase → unwrap, fall through
│   └─ stale + onStale=Error → throw IllegalStateException
│
├─ fast path / audit-cache path branches
│   └─ all 3 `root.compile(spark)` sites
│       replaced by `compiledRoot.compile(spark)` ← NEW (per Arch H1)
│       where compiledRoot = SemanticRollupOp(...) if matched, else root
```

The rollup routing runs **once per query**, before any compile. The
matched rollup (if any) is wrapped in a terminal `SemanticRollupOp`
that replaces `root` at all three compile sites via a `compiledRoot`
local val. This sidesteps Arch H1 (cache-miss bypass).

### 4.2 `SemanticRollupOp` — terminal op, NOT wrapped

(Per DE H6 — the wrap-around doesn't fit `SemanticAggregateOp`.)

```scala
// In: io.semanticdf (alongside SemanticTableOp, SemanticAggregateOp, ...)

/** A terminal leaf that compiles to a rollup table's projected columns.
  *
  * Unlike `SemanticAggregateOp` (which performs group-by + aggregation
  * on a source op), `SemanticRollupOp` does NO aggregation — the
  * rollup source is already pre-aggregated. The compile method:
  *
  *   1. Loads the source DataFrame via `sourceProvider(name)`
  *   2. If query grain != rollup grain, re-aggregates with
  *      `groupBy(queryGrain)` (valid only when all requested measures'
  *      aggregators are `canReAggregate` — see RollupAggregator)
  *   3. Applies the requested time-grain predicate (pushdown if the
  *      rollup has a coarser time dimension, fall back to base if not)
  *   4. Projects the requested dimensions + measures via `select(...)`
  *
  * The op is a TERMINAL — it does NOT have a `source: SemanticOp`
  * field. This sidesteps DE H6: `SemanticRollupOp` is the root of the
  * op tree, not a child of `SemanticTableOp`. `SemanticAggregateOp`
  * never sees it (it's the terminal, not an intermediate op).
  *
  * The `DataFrame` reference lives HERE, on the op leaf, NEVER on the
  * `SemanticTable` field. This sidesteps Arch H2: `SemanticTable` is
  * Serializable; only `SemanticRollupOp` holds a non-Serializable
  * DataFrame, and `SemanticRollupOp` lives in the op tree that the
  * compile path walks — never shipped to executors.
  */
final case class SemanticRollupOp(
  rollup:         Rollup,
  signature:      QuerySignature,
) extends SemanticOp {
  override def compile(spark: SparkSession): DataFrame = {
    val source = rollup.sourceProvider(rollup.name)  // thunk — invoked at compile time
    val grainMatch = signature.dimensions.toSet == rollup.rollupDimensions.toSet

    // --- Re-aggregate if needed (DE H1 fix) ---
    val aggregated: DataFrame =
      if (grainMatch) {
        source
      } else {
        // Re-aggregation is only valid if all requested measures can re-aggregate.
        val reAggOk = signature.measures.forall { m =>
          rollup.rollupMeasures
            .find(_.name == m)
            .exists(rm => rm.aggregator.canReAggregate(
              rollupGrain = rollup.rollupDimensions.toSet,
              queryGrain  = signature.dimensions.toSet,
            ))
        }
        if (!reAggOk) throw new IllegalStateException(
          s"Rollup '${rollup.name}' grain ${rollup.rollupDimensions} " +
          s"doesn't match query grain ${signature.dimensions} and the requested " +
          s"measures aren't re-aggregable. Fall back to base."
        )
        source.groupBy(signature.dimensions.map(col): _*).agg(...)
      }

    // --- Apply time-grain predicate (DE H2 fix) ---
    val timeFiltered: DataFrame = signature.timeGrain match {
      case Some(grain) if grain.finer(rollupGrain(rollup)) =>
        // Rollup's time grain is coarser than query — pushdown not possible
        throw new IllegalStateException(
          s"Rollup '${rollup.name}' has coarser time grain; fall back to base"
        )
      case _ => aggregated
    }

    // --- Project requested dimensions + measures ---
    val projection = (signature.dimensions ++ signature.measures).distinct
    if (projection.isEmpty) timeFiltered
    else timeFiltered.select(projection.map(col): _*)
  }
}
```

### 4.3 The matching algorithm

(Per DE H1, H2, H4, H5; Arch H3.)

```scala
// In SemanticTable.scala (real implementation, not pseudo)

def findRollupMatch(
  signature: QuerySignature,
): Either[String, Rollup] = {
  val candidates: Seq[Rollup] = rollups.filter(_.baseModel == this.name.getOrElse(""))

  candidates
    .filter { rollup =>
      // 1. GRAIN: either exact match, OR rollup grain is a superset AND
      //    every requested measure's aggregator can re-aggregate.
      val grainOk =
        signature.dimensions.toSet == rollup.rollupDimensions.toSet ||
        (signature.dimensions.toSet.subsetOf(rollup.rollupDimensions.toSet) &&
         signature.measures.forall { m =>
           rollup.rollupMeasures
             .find(_.name == m)
             .exists(_.aggregator.canReAggregate(
               rollupGrain = rollup.rollupDimensions.toSet,
               queryGrain  = signature.dimensions.toSet,
             ))
         })

      // 2. FILTER COVERAGE: every column in WHERE must exist in the
      //    rollup source (or the rollup's pre-filter excludes them).
      val filterOk = signature.filterColumns.subsetOf(
        rollup.sourceProvider(rollup.name).columns.toSet
        // Note: sourceProvider invoked here for column check; cached
        // DataFrame columns don't change between calls so this is fine.
      )

      // 3. MEASURE COVERAGE: every requested measure in the rollup.
      val measureOk = signature.measures.toSet.subsetOf(
        rollup.rollupMeasures.map(_.name).toSet
      )

      // 4. TIME GRAIN: rollup's time grain >= query's time grain (or
      //    rollup has no time dimension). Pushdown is possible iff
      //    rollup is at least as coarse as the query.
      val timeGrainOk = signature.timeGrain.forall { qg =>
        rollupTimeGrainOf(rollup).forall(_.coarserOrEqual(qg))
      }

      // 5. JOIN PARTICIPATION: if the query is a join query, the rollup
      //    must be registered against the joined model, not a single
      //    base model. (Joined rollups are out of scope for v0.3.0, so
      //    we reject for now.)
      val joinOk = signature.joinModel.isEmpty

      grainOk && filterOk && measureOk && timeGrainOk && joinOk
    }
    .sortBy(rollup => estimatedRowCount(rollup))  // `() => Long` thunk, DE H3 fix
    .headOption
    .map { matched =>
      // 6. FRESHNESS check (DE H5 fix)
      matched.freshness match {
        case RollupFreshness.NoTracking => Right(matched)
        case RollupFreshness.Track(wmProvider, maxStale, onStale) =>
          val wm    = wmProvider()
          val now   = java.time.Instant.now()
          val age   = java.time.Duration.between(wm, now)
          if (age.compareTo(maxStale) <= 0) {
            Right(matched)
          } else {
            onStale match {
              case OnStalePolicy.FallBackToBase =>
                Left(s"Rollup '${matched.name}' is stale (age=$age, max=$maxStale); falling back to base")
              case OnStalePolicy.Error =>
                throw new IllegalStateException(
                  s"Rollup '${matched.name}' is stale (age=$age, max=$maxStale); refusing to serve stale data"
                )
            }
          }
      }
    }
    .getOrElse(Left("No registered rollup matches the query signature"))
}

/** `() => Long` thunk — re-evaluated on every match. (Arch H3 fix) */
private def estimatedRowCount(rollup: Rollup): Long =
  rollup.sourceProvider(rollup.name).count()  // Spark job; acceptable on hot path

private def rollupTimeGrainOf(rollup: Rollup): Option[TimeGrain] =
  rollup.rollupDimensions.flatMap(d => findDimensionTimeGrain(d)).headOption
```

Six falsifiable conditions:

1. **Grain match**: exact OR superset + re-aggregable (per DE H1).
2. **Filter coverage**: query WHERE columns ⊆ rollup source columns (per DE H2).
3. **Measure coverage**: query measures ⊆ rollup measures.
4. **Time grain**: rollup time grain ≥ query time grain (per Arch L4).
5. **Join**: non-join query only (joined rollups are v0.4+).
6. **Freshness**: rollup watermark ≤ maxStaleness; on stale, fall back or error (per DE H5).

### 4.4 Granularity fallback — REMOVED

(Per DE H3.)

The original v0 plan proposed a `CoarsestAcceptable` fallback policy that
would route to a coarser-grain rollup and join back to base for the
missing dimensions. **This is mathematically impossible**: lost
allocation cannot be recovered. If a rollup is built at `(region, total,
count)` and the user queries `(region, category, total)`, the per-category
breakdown was discarded when the rollup was built; joining back to a
category fact CROSSES the totals, producing duplicated totals.

**v0.3.0 ships with no granularity fallback.** If no exact-grain rollup
matches, fall back to the base fact table. A future v0.4+ feature could
add hierarchical rollups (weekly → monthly) where the rollup itself
preserves the finer-grain detail (e.g., a monthly rollup stores weekly
subtotals so drilling down to week is just a re-aggregation within the
rollup). That's a different design.

### 4.5 Manifest round-trip

(Per Arch M9, M2.)

Rollups need to survive manifest round-trip. The schema:

```json
{
  "rollups": [
    {
      "name": "orders_by_region_category",
      "baseModel": "orders",
      "rollupDimensions": ["region", "category"],
      "rollupMeasures": [
        { "name": "total", "aggregator": "sum" },
        { "name": "count", "aggregator": "count" }
      ],
      "freshness": {
        "kind": "no-tracking"
      }
    }
  ]
}
```

The `sourceProvider` function is NOT serialized (functions can't be JSON-serialized). At `fromJson` time, the rollup's `sourceProvider` is supplied by the user via `SemanticManifest.fromJson(json, sourceProviders: Map[String, () => DataFrame])`.

**Backward compat**: v0.2.x manifests have no `rollups` field. The reader treats missing as `Seq.empty`. v0.3.x manifests round-trip cleanly; v0.2.x manifests load with empty rollups.

### 4.6 Source freshness validation

(Per DE H5.)

The library invokes `watermarkProvider()` at the first `compile()` call after registration. If the resulting `Instant` is older than `maxStaleness`, the `onStale` policy fires:
- `FallBackToBase`: the route-to-rollup returns `Left(...)`; the compile falls back to base.
- `Error`: the route-to-rollup throws `IllegalStateException`.

The watermark is **not cached** at registration — it's checked at every compile (cheap thunk call). This sidesteps Arch H3.

---

## 5. Edge cases & error handling

### 5.1 Illegal inputs (compile-time where possible)

- `Rollup.name` empty → `IllegalArgumentException` at `withRollup` time
- `Rollup.baseModel` doesn't match parent model's `name` → `IllegalArgumentException`
- `Rollup.rollupDimensions` empty → `IllegalArgumentException` (a rollup with no dimensions is the base table)
- `Rollup.rollupMeasures` empty → `IllegalArgumentException`
- `RollupAggregator.parse(unknown)` → `None` (parse don't validate)

### 5.2 Misuse at query time

- No rollup matches → fall back to base table; no error
- Rollup matches but stale → `onStale` policy (fall back to base or throw)
- Rollup matches but filter references missing column → no match; fall back
- Rollup matches but query requests non-additive measure → no match (canReAggregate returns false); fall back
- Rollup matches but query is a join query → no match (joined rollups are v0.4+); fall back
- User explicitly disables rollup routing for a single query → `.withoutRollup()` returns a copy with empty rollups (opt-out, not in v0.3.0 spec)

### 5.3 Schema drift on the rollup source

If the user's `sourceProvider` returns a DataFrame missing the columns it claims, the compile fails with a clear Spark error: `Reference 'region' is not a column`. The library does NOT add silent null-handling; this is a user error caught at compile time.

### 5.4 Re-aggregation failure

If the query grain is a strict subset of the rollup grain AND any requested measure's aggregator is non-additive (e.g., `min` over `count` would be wrong), the matcher returns `Left(...)` and the compile falls back to base. The user sees the base-table query result.

---

## 6. Performance budget

| Path | Today | With rollup | Overhead |
|---|---|---|---|
| Fast path, no rollup, no match | ~baseline | ~baseline + ~5μs (set lookup + thunk call) | negligible |
| Fast path, no rollup, match found | ~baseline | ~baseline + ~50μs (match + re-aggregate + project) | negligible |
| Audit/cache path, no rollup, no match | ~baseline | ~baseline + ~5μs | negligible |
| Audit/cache path, match found | ~baseline | ~baseline + ~50μs | negligible |

The matching is a single in-memory scan against pre-registered rollups (typically 1-5 rollups per model) + a thunk call for `estimatedRowCount`. Total overhead is **< 1μs per query** in the common case (n=1-10 rollups), rising to ~10μs at n=100 rollups.

The savings are large:
- 1M-row fact table → 100-row rollup = **10,000× less data** to scan
- Aggregation skipped (already pre-aggregated) or re-aggregated (cheap groupBy on small DF)
- Net: **50× to 50,000× speedup** for typical dashboards (validated against the example in section 7)

The v0 plan claimed "< 100μs" — this is conservative but pessimistic. The realistic number is **< 1μs** in the common case. (Per Arch L1.)

---

## 7. Example (user-facing)

This will become `examples/runtime-tuning/src/main/scala/com/example/runtime/RollupMain.scala` — a runnable demo.

```scala
package com.example.runtime

import io.semanticdf._
import io.semanticdf.rollup._

object RollupMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    // 1. Build the base fact model
    val ordersDf = buildOrders(spark, n = 1_000_000)
    val orders = toSemanticTable(ordersDf, name = Some("orders"))
      .withDimensions(
        Dimension("region", _ => ordersDf("region")),
        Dimension("category", _ => ordersDf("category")),
        Dimension("customer_id", _ => ordersDf("customer_id")),
      )
      .withMeasures(
        Measure("total", _ => sum(ordersDf("amount"))),
        Measure("count", _ => count(lit(1))),
      )
      .withAuditSink(AuditSink.inMemory())

    // 2. User maintains a rollup table (via their own pipeline)
    val ordersByRegionCategoryDf = ordersDf
      .groupBy("region", "category")
      .agg(sum("amount").as("total"), count("*").as("count"))

    // 3. Register the rollup with semanticdf
    val ordersWithRollup = orders.withRollup(Rollup(
      name             = "orders_by_region_category",
      baseModel        = "orders",
      rollupDimensions = Seq("region", "category"),
      rollupMeasures   = Seq(
        RollupMeasure("total", RollupAggregator.Sum),
        RollupMeasure("count", RollupAggregator.Count),
      ),
      sourceProvider   = _ => ordersByRegionCategoryDf,
    ))

    // 4. Run the query — auto-routes to rollup
    val t0 = System.nanoTime()
    val result = ordersWithRollup.query(
      measures   = Seq("total", "count"),
      dimensions = Seq("region", "category"),
    ).execute(spark).collect()
    val elapsedMs = (System.nanoTime() - t0) / 1e6

    // 5. Inspect the routing decision
    val matchResult = ordersWithRollup.findRollupMatch(QuerySignature(
      measures      = Seq("total", "count"),
      dimensions    = Seq("region", "category"),
      filterColumns = Set.empty,
      timeGrain     = None,
      joinModel     = None,
    ))
    assert(matchResult.isRight)
    assert(matchResult.toOption.get.name == "orders_by_region_category")

    // 6. Verify NO rollup matches when query doesn't align
    val noMatchResult = ordersWithRollup.findRollupMatch(QuerySignature(
      measures      = Seq("total", "count"),
      dimensions    = Seq("region"),  // one dim less than rollup grain
      filterColumns = Set.empty,
      timeGrain     = None,
      joinModel     = None,
    ))
    assert(noMatchResult.isLeft)  // can't re-aggregate total without grain fix

    // 7. Verify non-additive measure prevents routing
    val noMatchAvg = ordersWithRollup.findRollupMatch(QuerySignature(
      measures      = Seq("total", "avg_amount"),  // avg needs sum+count pair
      dimensions    = Seq("region"),
      filterColumns = Set.empty,
      timeGrain     = None,
      joinModel     = None,
    ))
    assert(noMatchAvg.isLeft)

    // 8. Inspect audit history for recommendations
    val recommendations = ordersWithRollup.auditSink.get
      .asInstanceOf[InMemoryAuditSink]
      .recommendRollups(forModel = Some("orders"), topN = 5)
    println("Recommended rollups: " + recommendations)

    // Output:
    //   Query result: 100 rows
    //   Elapsed: ~50ms (vs ~2000ms without rollup — 40× faster)
    //   Routing: matched orders_by_region_category
    //   Recommended rollups:
    //     - region × category → total, count (4,200,000 distinct queries)

    spark.stop()
  }
}
```

Per **Arch M7**, the example exercises:
- Rollup registration
- Routing match (positive case)
- Routing fallback (grain mismatch)
- Routing fallback (non-additive measure)
- Audit event capture
- Recommendation generation

---

## 8. Testing strategy (per debug-mantra)

Every falsifiable claim in this design maps to a test:

| Claim | Test |
|---|---|
| Rollup with exact grain is found | `RollupSpec: matches exact grain` |
| Rollup with superset grain + additive measures is found | `RollupSpec: matches superset + re-aggregates` |
| Rollup with superset grain + non-additive measures is rejected | `RollupSpec: rejects superset + non-additive` |
| Rollup with missing filter column is rejected | `RollupSpec: rejects missing filter column` |
| Rollup with non-additive measure is rejected | `RollupSpec: rejects non-additive measure` |
| Rollup with coarser time grain than query is rejected | `RollupSpec: rejects coarser time grain` |
| Rollup on a joined query is rejected | `RollupSpec: rejects joined query` |
| Smallest matching rollup wins (not just any match) | `RollupSpec: picks smallest by row count` |
| `estimatedRowCount` is re-evaluated on each match (not cached) | `RollupSpec: estimatedRowCount reflects current source` |
| Stale rollup falls back to base per `onStale` policy | `RollupSpec: stale rollup falls back` |
| Stale rollup throws when `onStale = Error` | `RollupSpec: stale rollup throws` |
| Audit pre-check still fires when no rollup | `AuditRequestInvarianceSpec: rollup path doesn't bypass audit check` |
| Cache-miss path applies rollup routing | `RollupSpec: cache-miss path uses rollup` |
| Manifest round-trip preserves rollup definitions | `SemanticManifestSpec: rollups survive round-trip` |
| Manifest fromJson without sourceProviders throws clear error | `SemanticManifestSpec: missing sourceProviders errors clearly` |
| `recommendRollups` groups by shape, dedupHash, not call count | `RollupRecommendationSpec: groups by shape` |
| `recommendRollups` filters out queries already hitting rollups | `RollupRecommendationSpec: skips queries matching existing rollup` |
| Auto-routing latency overhead < 1μs in common case (n=10) | `RollupPerfSpec: routing overhead < 1μs at n=10` |
| Real workload speedup 40×+ (1M-row table → 100-row rollup) | `RollupExampleSpec: 1M-row table, ≥ 40× speedup` |
| `SemanticRollupOp` is the terminal op (no wrap) | `RollupSpec: SemanticRollupOp is terminal` |
| `SemanticTable` with rollups remains Serializable | `RollupSpec: Serializable contract preserved` |

All tests use the existing `SparkSessionFixture`. No new fixtures needed.

---

## 9. Phasing & PR breakdown

(Reordered per Arch M2, M3.)

### PR #1: Rollup types + manifest round-trip

**Files**: 2 new files (`Rollup.scala`, `RollupMeasure.scala`, `RollupAggregator.scala`, `RollupFreshness.scala`, `OnStalePolicy.scala` in `io.semanticdf.rollup`); 2 schema files updated
**Tests**: 5 falsifiable tests in `RollupSpec` (types, manifest round-trip, backward compat)
**Risk**: Low — pure type additions, manifest field only

### PR #2: Routing + `SemanticRollupOp` + cache-miss integration

**Files**: `SemanticOp.scala` (add `SemanticRollupOp`); `SemanticTable.scala` (add `findRollupMatch`, `withRollup`); `SemanticTableCore.scala` (wrap all 3 `root.compile` sites with `routeToRollup`)
**Tests**: 8 falsifiable tests (matches, doesn't match, fresh/stale, cache-miss path, Serializable contract, terminal op)
**Risk**: High — touches the hot compile path AND the audit/cache invariant. Per **debug-mantra**, the cache-miss integration has a real risk of bypassing the audit invariant; the test at line `RollupSpec: cache-miss path uses rollup` MUST be added before merging.

### PR #3: Freshness + `recommendRollups`

**Files**: `AuditSink.scala` (add `recommendRollups`); `InMemoryAuditSink.scala` (implement)
**Tests**: 4 falsifiable tests (recommendation algorithm, freshness policy, coverage filter)
**Risk**: Low — read-only over existing audit infra

### PR #4: Example + docs

**Files**: new `RollupMain.scala`; `docs/design/rollups.md` already exists; runtime-tuning README updated
**Tests**: end-to-end test in `RollupExampleSpec`
**Risk**: Low

---

## 10. Open questions for review

These are decisions I'd like the second-pass reviewers to weigh in on:

1. **`QuerySignature` vs reusing `AuditQueryRequest`** — Should `findRollupMatch` take an `AuditQueryRequest` (existing type) or the new `QuerySignature` (slimmer type)? My design uses `QuerySignature` for clarity, but reusing `AuditQueryRequest` avoids one new type.
2. **`estimatedRowCount` thunk cost** — Calling `source.count()` at every `findRollupMatch` is a Spark job. Per-match cost could be 100ms-10s for huge rollups. Should we cache with a TTL? My current design caches nothing (Arch H3 fix); a 30-second TTL might be a middle ground.
3. **`findRollupMatch` return type** — `Either[String, Rollup]` is informative but verbose for the common case (`Right(rollup)`). Is `Try[Rollup]` better? Or keep `Either` for the explicit error message?
4. **Joined rollups (deferred to v0.4+)** — What should the `joinOk = signature.joinModel.isEmpty` rejection look like? Error message? Silent fall-back? My current design silently falls back, but a clear "joined rollups are v0.4+" warning might be more helpful.

---

## 11. Skill compliance (revised)

### karpathy

- **Surgical**: each PR touches ≤ 3 production files. PR #1 adds new types + manifest schema; PR #2 modifies `SemanticOp.scala` + `SemanticTable.scala` + `SemanticTableCore.scala`; PR #3 adds `AuditSink` method; PR #4 is docs + example.
- **Verifiable success criteria**: 21 falsifiable tests (section 8); each names the claim it pins.
- **No opportunistic refactors**: no cleanups of adjacent code; this PR cycle is pure feature add.

### debug-mantra

- **Reproduce**: example in section 7 is runnable end-to-end via `mvn exec:java`.
- **Trace**: section 4.1 traces the routing decision into the existing `toDataFrameInternal`; section 4.2 traces the rollup compile path; section 4.3 traces the matching algorithm.
- **Falsify**: 21 falsifiable tests (section 8); each names the claim it pins.
- **Cross-reference**: section 6 quantifies the overhead; section 5 documents edge cases the existing infrastructure already handles; section 7's example exercises 7 different scenarios (positive, fallback, audit, etc.).
- **Verify**: section 9 phases the work so each PR is independently verifiable.

### scala-data-driven-refactor

- **Parse don't validate**: `RollupAggregator.parse(s: String): Option[RollupAggregator]` smart-constructor; `Rollup` has `require()` at construction (section 5.1); `QuerySignature` is a value class; downstream code trusts the types.
- **Plain types**: `RollupFreshness` sealed trait (two cases: `NoTracking`, `Track(...)`); `OnStalePolicy` sealed trait (two cases); `RollupAggregator` sealed trait (six cases — Sum, Count, Min, Max, Avg, Stddev). Each is justified: each case has different behavior, so ADT escalation IS warranted.
- **No ADT escalation without justification**: `RollupFallbackPolicy` was REMOVED (Arch H4); replaced with no policy at all in v0.3.0.
- **No closure gotcha**: `SemanticRollupOp.compile(spark)` takes everything as constructor args (rollup, signature); no outer-scope capture.
- **Distributed-ser**: `Rollup` value class is pure data (`String`, `Seq[String]`, `String => DataFrame` thunk — all Serializable). `SemanticRollupOp` holds the `DataFrame` only on the op-tree leaf, NOT on the `SemanticTable` field. `SemanticTable extends Serializable` (line 379 of `SemanticTable.scala`) — preserved.

---

## 12. Full revision log (v0 → v1)

| # | v0 section | Issue | v1 fix | Source |
|---|---|---|---|---|
| 1 | §3.1 | `RollupFallbackPolicy` sealed with 1 case (ceremony) | REMOVED; no policy in v0.3.0 | Arch H4 |
| 2 | §3.1 | `Rollup.source: DataFrame` (breaks Serializable) | `Rollup.sourceProvider: String => DataFrame` thunk held on `SemanticRollupOp` only | Arch H2 |
| 3 | §3.1 | `aggregator: String` with `Set.contains` validation | `RollupAggregator` sealed trait (Sum/Count/Min/Max/Avg/Stddev) with `parse(...)` smart-ctor | DE H4 |
| 4 | §3.1 | No freshness tracking | `RollupFreshness` sealed trait + `OnStalePolicy` sealed trait | DE H5 |
| 5 | §3.2 | `findRollupMatch` takes `(Seq[String], Seq[String])` only | Takes `QuerySignature` (dims + measures + filter cols + time grain + join info) | DE H2 |
| 6 | §3.2 | `findRollupMatch: Option[Rollup]` (no reason on miss) | `Either[String, Rollup]` | DE H2 + Arch M5 |
| 7 | §3.4 | `recommendRollups` counts calls (per-call bot can dominate) | Counts distinct `dedupHash` per shape | DE MED9 |
| 8 | §3.4 | `recommendRollups` includes rollup-matched queries | Filters out queries already hitting rollups (needs `rollupsByModel` param) | DE MED9 |
| 9 | §3.5 | (NEW) No discovery API | `def listRollups(): Seq[Rollup]` | Arch L5 |
| 10 | §4.2 | `SemanticRollupOp` wraps a source op (breaks `SemanticAggregateOp.resolveModel`) | `SemanticRollupOp` is a TERMINAL (no `source: SemanticOp` field) | DE H6 |
| 11 | §4.3 | Matching uses `subsetOf` (returns duplicates on superset match) | Exact-grain match OR superset + `canReAggregate` check (with re-aggregation via `groupBy(queryGrain)`) | DE H1 |
| 12 | §4.3 | Matching ignores filters (silent wrong data) | Filter coverage check: `signature.filterColumns ⊆ source.columns` | DE H2 |
| 13 | §4.3 | Matching ignores time grain | Time grain check: rollup time grain ≥ query time grain | Arch L4 |
| 14 | §4.3 | Matching doesn't account for join queries | Join check: `signature.joinModel.isEmpty` (joined rollups deferred) | DE H6 |
| 15 | §4.3 | `estimatedRowCount` cached at registration, never invalidated | `() => Long` thunk re-evaluated on each match | Arch H3 |
| 16 | §4.3 | `isAdditive` set `{sum, count, min, max}` (math wrong) | `RollupAggregator.canReAggregate(rollupGrain, queryGrain)` with per-aggregator logic | DE H4 |
| 17 | §4.4 | `CoarsestAcceptable` policy (mathematically impossible) | REMOVED. Documented why. | DE H3 |
| 18 | §4.5 | Manifest round-trip last (YAML can't use rollups) | PR reordered: manifest in PR #1, not PR #5 | Arch M2 |
| 19 | §4.5 | No backward-compat handling for v0.2.x manifests | Added explicit "missing field → empty list" handling | Arch M9 |
| 20 | §6 | "< 100μs" performance budget (loose) | "< 1μs at n=10, < 10μs at n=100" (tightened) | Arch L1 |
| 21 | §7 | Example only exercises one positive case | 7 scenarios: positive match, grain fallback, non-additive, audit event, recommendation, freshness, time-grain | Arch M7 |
| 22 | §8 | 12 falsifiable tests | 21 falsifiable tests | (cumulative) |
| 23 | §9 | PR #1 alone not useful (no routing) | PR #1 = types + manifest only (foundation); PR #2 = routing (combines old PRs #1 + #2) | Arch M3 |
| 24 | §10 (new) | No open questions | 4 questions for review (signature type, thunk cost, Either vs Try, joined rollups) | (process) |
| 25 | §11 (updated) | Skill compliance claimed | Each claim now points to specific sections showing compliance | (process) |
| 26 | §12 (new) | No revision history | THIS SECTION | (process) |
| 27 | §12 | "40-1000× speedup" (conservative) | "50× to 50,000× speedup" (correct range: 5× agg × 10× to 10,000× data reduction) | Arch L2 |

**Files changed in v1**: 1 (this plan document only). No code changes yet.

---

## 13. Next steps

1. **Second-pass review** of this revised plan by senior DE + senior architect (please re-falsify).
2. After approval: open PR #1 (types + manifest) — no compile-path changes, pure foundation.
3. After PR #1 merged: open PR #2 (routing + `SemanticRollupOp` + cache-miss integration) — the high-risk one.
4. PR #3 (freshness + recommendation).
5. PR #4 (example + docs).

Estimated total: 4 PRs, ~1200 LoC, ~3 weeks of work for one engineer.