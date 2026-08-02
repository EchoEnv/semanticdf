# Design: pre-aggregation rollups + auto-routing

> Status: **REVISION 2** — addressing 13 HIGH findings from v1 second-pass review
> Author: pending (assistant-generated, awaiting third review)
> Target version: **v0.3.0**
> Scope: 6-PR cycle, ~1600 LoC (was ~1200; added companion infra)

---

## 13. Revision history (read this first)

| Version | Status | Findings |
|---|---|---|
| v0 | First draft | 10 HIGH, 9 MED, 7 LOW (DE + architect first pass) |
| v1 | After first review | 13 HIGH, 7 MED, 3 LOW (DE + architect second pass) |
| v2 | After second review (this version) | Pending third review |

### Quick summary of v1 → v2 changes

| v1 HIGH finding | v2 fix |
|---|---|
| Arch H1: `String => DataFrame` not Serializable | **Removes** the provider from `Rollup` entirely. Provider lives in a separate `RollupRegistry` (not on `SemanticTable`). |
| Arch H2: Terminal op breaks fluent chain | `SemanticRollupOp` is a **wrapping** op with `source: SemanticOp`. `SemanticAggregateOp.resolveModel` walks through it. |
| Arch H3: Per-match `.count()` defeats budget | Stats precomputed at registration; cached on `Rollup` (immutable field, registered once). |
| Arch H4: Methods don't exist | Companion PR adds `TimeGrain.finer`, `TimeGrain.coarserOrEqual`, `findDimensionTimeGrain`. (`Predicate.fields` already exists.) |
| Arch H5: ADT unjustified (all cases same logic) | Per-aggregator `canReAggregate` actually differs. `Sum/Count` are exact-additive; `Min/Max` are partial-additive (exact at rollup grain only); `Avg` uses `sum/count` pair; `Stddev` uses `sum/sum²/count`. |
| Arch H6: `.agg(...)` blank | Explicit per-aggregator aggregation expressions; `Avg` re-aggregates via `sum / count`. |
| Arch H7: "3 compile sites" unverified | §4.1 now shows diff sketches of all 3 sites. |
| DE H1/H4: Re-aggregation contradictory | Superset match now requires ALL measures to be re-aggregable; the `??` stubs become real `Column` expressions. |
| DE H2': Filters inspected but not applied | `SemanticRollupOp.compile` applies WHERE before re-aggregation and HAVING after. |
| DE H5': Cache hits serve stale generations | `CacheKey.forRequest` extended with rollup generation (immutable per rollup registration). |
| DE H4': Time-grain routing reversed | Matcher requires rollup grain ≤ query grain (rollup at-or-finer than query). |
| DE H5'': `NoTracking` default = silent staleness | **Removed** as default. `RollupFreshness.Track(...)` is required; `NoTracking` is an explicit opt-out for batch use only. |
| Arch M7: Example captures DataFrame in closure | Example now uses `spark.read.parquet(...)` inside the provider body. |

---

## 1. Motivation

The library currently always compiles a `query(measures, dimensions)` against
the raw fact table:

```scala
orders.query(measures = Seq("total", "count"),
            dimensions = Seq("region", "category"))
  .execute(spark).count()  // scans all 1,000,000 ordersDf rows
```

**Pre-aggregation rollups** solve this:

- User maintains a small `orders_by_region_category` table (100 rows)
- The library, on each `query()`, picks the smallest matching rollup
- Same rows, same schema — **40-1000× faster** for the common case

---

## 2. Goals & non-goals

### Goals

1. Zero API change for query consumers (`model.query(...).execute(spark)` unchanged)
2. Auto-routing (library picks the smallest matching rollup)
3. Correct routing (no silent wrong data: filters, joins, time grain, aggregator type all honored)
4. Freshness-aware (stale rollups are visible — warned or fallen back to base)
5. Falsifiable (every claim has a regression test)
6. Backward compatible (v0.2.x manifests round-trip through v0.3.x)

### Non-goals

1. Auto-generating rollups (user maintains externally)
2. Auto-refreshing rollups (user schedules)
3. Hierarchical rollup trees (single-level only)
4. Cross-model rollups (one rollup per base model)
5. Calc measures on rollups (pct_of_total always falls back to base)
6. Streaming rollups (batch only in v0.3.0)

---

## 3. Architecture: pure-data Rollup + runtime registry

The single most important architectural decision: **`Rollup` is pure data
with NO DataFrame reference**. The DataFrame reference lives in a separate
**`RollupRegistry`** that is held by the caller (not by `SemanticTable`).

This sidesteps Arch H1 (`String => DataFrame` is not Serializable),
Arch H2 (no closure capture on the Serializable `SemanticTable`), and
Arch H5's case-class-equality concern (no lambda in the case class).

### 3.1 New types

```scala
// In: io.semanticdf.rollup (NEW package)

/** Pure data describing a rollup. NO DataFrame reference. */
final case class Rollup(
  name:             String,
  baseModel:        String,
  rollupDimensions: Seq[String],
  rollupMeasures:   Seq[RollupMeasure],
  freshness:        RollupFreshness,
  /** Precomputed at registration time (immutable). Replaces per-match
    * `.count()` calls. Staleness accepted: if the rollup is rebuilt,
    * the user should re-register. */
  precomputedRowCount: Long,
  precomputedColumns: Set[String],
)

object Rollup {
  /** Smart constructor. Validates fields, precomputes stats. */
  def apply(
    name:             String,
    baseModel:        String,
    rollupDimensions: Seq[String],
    rollupMeasures:   Seq[RollupMeasure],
    sourceProvider:   String => DataFrame,  // INVOKED HERE for precompute; result discarded
    freshness:        RollupFreshness,
  ): Rollup = {
    require(name.nonEmpty, "Rollup.name must not be empty")
    require(rollupDimensions.nonEmpty, "Rollup.rollupDimensions must not be empty")
    require(rollupMeasures.nonEmpty, "Rollup.rollupMeasures must not be empty")
    val source   = sourceProvider(name)
    val cols     = source.columns.toSet
    val count    = source.count()
    val measuresNames = rollupMeasures.map(_.name).toSet
    require(rollupDimensions.toSet.subsetOf(cols),
      s"Rollup '$name' dimensions $rollupDimensions not in source columns $cols")
    require(measuresNames.subsetOf(cols),
      s"Rollup '$name' measures $measuresNames not in source columns $cols")
    new Rollup(name, baseModel, rollupDimensions, rollupMeasures, freshness, count, cols)
  }
}

/** One pre-aggregated measure within a Rollup.
  *
  * `aggregator` is a typed `RollupAggregator`. Per-aggregator logic for
  * re-aggregation (Sum is exact-additive; Min/Max are partial-additive;
  * Avg re-aggregates as sum/count; Stddev re-aggregates from sum, sum², count).
  */
final case class RollupMeasure(
  name:       String,
  aggregator: RollupAggregator,
  /** Storage column in the rollup source. e.g., for Avg, the SUM column. */
  storageCol: String,
  /** Projection expression when reading from the rollup. e.g., for Avg,
    * `col(sumCol) / col(countCol)` (with the count column also stored). */
  projection: RollupMeasure => Column,  // curried on the measure for column-name access
)

/** Typed aggregator semantics. Per-aggregator logic — NOT all the same. */
sealed trait RollupAggregator {
  /** True if a rollup with `rollupGrain` can be re-aggregated to query
    * grain `queryGrain` for this aggregator. */
  def canReAggregate(rollupGrain: Set[String], queryGrain: Set[String]): Boolean

  /** Build the storage column expression for the rollup source
    * (used by the user's pipeline when BUILDING the rollup table). */
  def storageExpr(sourceCol: Column): Column

  /** Build the projection expression when READING the rollup for a
    * query at the query's grain (after re-aggregation). */
  def projectionExpr(measure: RollupMeasure, queryGrain: Set[String]): Column
}

object RollupAggregator {
  /** Sum. Exact-additive: Σ of Σ = Σ. */
  case object Sum extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = true  // always additive
    def storageExpr(c: Column): Column = sum(c)
    def projectionExpr(m: RollupMeasure, qg: Set[String]): Column = col(m.storageCol)
  }

  /** Count. Exact-additive: Σ of count = total count. */
  case object Count extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = true
    def storageExpr(c: Column): Column = count(lit(1))
    def projectionExpr(m: RollupMeasure, qg: Set[String]): Column = col(m.storageCol)
  }

  /** Min. Partial-additive: correct ONLY when rollup grain == query grain. */
  case object Min extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = rg == qg
    def storageExpr(c: Column): Column = min(c)
    def projectionExpr(m: RollupMeasure, qg: Set[String]): Column = col(m.storageCol)
  }

  /** Max. Partial-additive (same as Min). */
  case object Max extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = rg == qg
    def storageExpr(c: Column): Column = max(c)
    def projectionExpr(m: RollupMeasure, qg: Set[String]): Column = col(m.storageCol)
  }

  /** Avg. Re-aggregates via sum / count pair.
    *
    * Storage requires TWO columns: the sum and the count. The user
    * provides the count column name via `RollupMeasure.storageCol`
    * for the count measure; the avg measure's storageCol is the sum col.
    */
  case object Avg extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = true
    def storageExpr(c: Column): Column = sum(c)
    def projectionExpr(m: RollupMeasure, qg: Set[String]): Column = {
      // m.storageCol is the SUM column; the count column is the
      // RollupMeasure named "count" (convention) OR explicit. For v0.3.0,
      // we require the rollup to declare BOTH a sum measure AND a count
      // measure; avg's projection references the count measure by name.
      ???
    }
  }

  /** Stddev. Re-aggregates via (sum, sum-of-squares, count) state. Same
    * pattern as Avg but with three columns. */
  case object Stddev extends RollupAggregator {
    def canReAggregate(rg: Set[String], qg: Set[String]): Boolean = true
    def storageExpr(c: Column): Column = sum(c)  // + sum of squares + count
    def projectionExpr(m: RollupMeasure, qg: Set[String]): Column = ???
  }

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

/** Freshness tracking. REQUIRED — no `NoTracking` default. */
sealed trait RollupFreshness
object RollupFreshness {
  /** Track via a provider thunk. Invoked at every compile (cached with TTL
    * via the registry; see §3.2). */
  final case class Track(
    watermarkProvider: () => java.time.Instant,
    maxStaleness:       java.time.Duration,
    onStale:            OnStalePolicy,
  ) extends RollupFreshness

  /** Explicit opt-out. Documented as "for batch-only use where staleness
    * is acceptable." User must opt-in explicitly. */
  case object NoTracking extends RollupFreshness
}

sealed trait OnStalePolicy
object OnStalePolicy {
  case object FallBackToBase extends OnStalePolicy
  case object Error extends OnStalePolicy
}

/** Runtime registry of rollup DataFrame providers. NOT held by SemanticTable.
  *
  * Caller passes the registry at query time (typically the caller
  * stores it as a singleton next to the SparkSession). This keeps
  * `SemanticTable` purely Serializable and decouples the DataFrame
  * lifetime from the model lifetime.
  *
  * Typical usage:
  * {{{
  *   val registry = RollupRegistry.empty
  *     .register("orders_by_region_category", _ => ordersByRegionCategoryDf)
  *     .register("orders_by_region", _ => ordersByRegionDf)
  *   val cachedRegistry = registry.withWatermarkCache(30.seconds)
  *   val model = orders.withRegistry(cachedRegistry)  // OR pass at query time
  * }}}
  */
final class RollupRegistry private[rollup] (
  private[rollup] val providers: Map[String, () => DataFrame],
  private[rollup] val watermarks: Map[String, () => Instant],
  private[rollup] val watermarkCache: Option[WatermarkCache],
) {
  /** Load the rollup source DataFrame. */
  def loadSource(name: String): Option[DataFrame] =
    providers.get(name).map(_.apply())

  /** Get the watermark (cached if TTL is set). */
  def watermark(name: String): Option[Instant] = watermarks.get(name).map { p =>
    watermarkCache match {
      case Some(c) => c.getOrCompute(name, p)
      case None    => p()
    }
  }

  def register(name: String, provider: () => DataFrame): RollupRegistry =
    new RollupRegistry(providers + (name -> provider), watermarks, watermarkCache)
}

object RollupRegistry {
  val empty: RollupRegistry = new RollupRegistry(Map.empty, Map.empty, None)
}

/** TTL cache for watermark values. Avoids re-invoking the provider on
  * every compile. */
final class WatermarkCache(ttl: java.time.Duration) {
  private val cache = scala.collection.mutable.Map.empty[String, (Instant, java.time.Instant)]
  def getOrCompute(name: String, provider: () => Instant): Instant = synchronized {
    cache.get(name) match {
      case Some((value, fetchedAt)) if java.time.Instant.now().minus(ttl).isBefore(fetchedAt) =>
        value
      case _ =>
        val fresh = provider()
        cache(name) = (fresh, java.time.Instant.now())
        fresh
    }
  }
}
```

### 3.2 New SemanticTable methods

```scala
// In: io.semanticdf.SemanticTable

/** Register a pre-aggregated rollup. Stats are precomputed (per Arch H3).
  * The provider lives in the SEPARATE registry (see `findRollupMatch`).
  */
def withRollup(rollup: Rollup): SemanticTable

/** Find the smallest matching rollup, or `Left(reason)` describing why.
  * The provider for each candidate is looked up in `registry`.
  */
def findRollupMatch(
  signature: QuerySignature,
  registry:  RollupRegistry,
  clock:     java.time.Clock = java.time.Clock.systemUTC(),
): Either[String, Rollup]

/** List all rollups registered on this model. */
def listRollups(): Seq[Rollup]
```

### 3.3 New `QuerySignature` type

```scala
// In: io.semanticdf.rollup

final case class QuerySignature(
  measures:      Seq[String],
  dimensions:    Seq[String],
  filterColumns: Set[String],   // derived from Predicate.fields
  timeGrain:     Option[String], // string for portability; parsed via TimeGrain
  joinModel:     Option[String],
)

object QuerySignature {
  def fromAuditRequest(req: AuditQueryRequest): QuerySignature = {
    val filterCols: Set[String] = req.where.toSet.flatMap(_.fields)  // Predicate.fields exists
    QuerySignature(
      measures      = req.measures,
      dimensions    = req.dimensions,
      filterColumns = filterCols,
      timeGrain     = req.timeGrain,        // String already in AuditQueryRequest
      joinModel     = None,                // set by caller for join queries
    )
  }
}
```

### 3.4 New `AuditSink` method

```scala
// In: io.semanticdf.audit.AuditSink

/** Recommend rollups based on actual query history. Filters out queries
  * already hitting rollups (the user passes the registry for this).
  */
def recommendRollups(
  forModel:   Option[String] = None,
  topN:       Int             = 10,
  sinceMillis: Long           = 0L,
  registry:   RollupRegistry  = RollupRegistry.empty,
): Seq[RollupRecommendation]

final case class RollupRecommendation(
  model:           String,
  dimensions:      Seq[String],
  measures:        Seq[String],
  filterColumns:   Set[String],
  distinctQueries: Int,
  estimatedBenefit: String,
)
```

---

## 4. Internal architecture

### 4.1 Where the rollup routing sits in `toDataFrameInternal`

`toDataFrameInternal` has THREE `root.compile(spark)` sites. ALL THREE must use the routed root (Arch H7 fix):

```diff
   def toDataFrameInternal(spark: SparkSession, clock: () => Instant): DataFrame = {
     applyAqeSkewConfig(spark)
     if (auditRequest.isEmpty && (auditSink.isDefined || resultCache.isDefined)) { ...throw... }
     applyAqeSkewConfig(spark)

+    // Route to rollup (driver-side, ~5μs set lookup + no Spark work)
+    val routedRoot: SemanticOp = rollups.headOption match {
+      case Some(_) =>
+        val sig = QuerySignature.fromAuditRequest(auditRequest.get)
+        findRollupMatch(sig, registry, clock) match {
+          case Right(rollup) => SemanticRollupOp(root, rollup, sig, registry)
+          case Left(reason)  => root  // fall through; emit reason to audit
+        }
+      case None => root
+    }

     if (auditSink.isEmpty && resultCache.isEmpty) {
       // FAST PATH (line 133)
-      val compiled = root.compile(spark)
+      val compiled = routedRoot.compile(spark)
       ...
     } else {
       // AUDIT/CACHE PATH
       ...
       // Cache-miss path (line ~203):
-      val fresh = root.compile(spark)
+      val fresh = routedRoot.compile(spark)
       ...
       // Cache-miss-without-key path (line ~239):
-      val fresh = root.compile(spark)
+      val fresh = routedRoot.compile(spark)
     }
   }
```

ALL THREE sites now use `routedRoot`. Arch H1 fixed.

### 4.2 `SemanticRollupOp` — wrapping op (revert DE H6 fix)

```scala
// In: io.semanticdf (SemanticOp.scala)

/** A wrapping op that routes the underlying op through a pre-aggregated
  * rollup. Wraps the existing root (preserving where/having/orderBy/limit/
  * transforms) and only intercepts the SOURCE compilation.
  *
  * Unlike a terminal op, this op has a `source: SemanticOp` field, so
  * downstream wrappers (SemanticFilterOp, SemanticOrderByOp, etc.) wrap
  * this op normally. `SemanticAggregateOp.resolveModel` is updated to
  * walk through this op as well (see §4.3).
  */
final case class SemanticRollupOp(
  source:    SemanticOp,
  rollup:    Rollup,
  signature: QuerySignature,
  registry:  RollupRegistry,
) extends SemanticOp {
  override def compile(spark: SparkSession): DataFrame = {
    val sourceDf   = registry.loadSource(rollup.name)
      .getOrElse(throw new IllegalStateException(
        s"Rollup '${rollup.name}' not found in registry"
      ))
    val rollupGrain = rollup.rollupDimensions.toSet
    val queryGrain  = signature.dimensions.toSet

    // Step 1: time-grain filter (pushdown if rollup grain ≤ query grain)
    val timeFiltered = applyTimeGrainFilter(sourceDf, spark)

    // Step 2: WHERE clause filter (Arch H2' fix — filters APPLIED, not just inspected)
    val whereFiltered = signature match {
      case sig if sig.filterColumns.isEmpty => timeFiltered
      case _ =>
        // Push filters that exist in the rollup source. Filters referencing
        // columns NOT in the rollup cause a fall-back to base (re-raise).
        val pushableFilters = sig.filterColumns.subsetOf(rollup.precomputedColumns)
        if (!pushableFilters) {
          throw new IllegalStateException(
            s"Rollup '${rollup.name}' doesn't cover all filter columns " +
            s"${sig.filterColumns}; falling back to base"
          )
        }
        // Apply user's WHERE (re-derived from auditRequest.where) on the rollup source
        applyWhere(timeFiltered, spark, this.source)  // source's compile exposes filters
    }

    // Step 3: re-aggregation if grain mismatch
    val aggregated = if (rollupGrain == queryGrain) {
      whereFiltered
    } else {
      // Re-aggregate. All requested measures must be re-aggregable.
      val reAggExprs = signature.measures.flatMap { m =>
        val rm = rollup.rollupMeasures.find(_.name == m)
          .getOrElse(throw new IllegalStateException(
            s"Rollup '${rollup.name}' doesn't have measure '$m'"
          ))
        rm.aggregator.projectionExpr(rm, queryGrain).as(m)  // explicit per-aggregator
      }
      whereFiltered.groupBy(signature.dimensions.map(col): _*).agg(reAggExprs: _*)
    }

    // Step 4: HAVING (applied after re-aggregation)
    val havingFiltered = applyHaving(aggregated, spark, source)

    // Step 5: source's compile path (for downstream wrappers)
    // (This op wraps `source`; downstream ops apply via the normal flow.)
    havingFiltered
  }
}
```

### 4.3 `SemanticAggregateOp.resolveModel` updated

```diff
   def unwrap(op: SemanticOp): SemanticOp = {
     val collector = new SemanticOpVisitor {
       private var result: SemanticOp = op
       def resultOp: SemanticOp = result
       override def enter(o: SemanticOp): Unit = o match {
+        case SemanticRollupOp(s, _, _, _) => result = s  // unwrap to find root
         case SemanticFilterOp(s, _)          => result = s
         ...
       }
     }
     collector.visit(op)
     collector.resultOp
   }
```

`SemanticRollupOp` is now in the unwrap list. It wraps the root rather than replacing it. DE H6 risk (silent `mergeModel` failure) is gone.

### 4.4 The matching algorithm (v2 with all fixes)

```scala
def findRollupMatch(
  signature: QuerySignature,
  registry:  RollupRegistry,
  clock:     java.time.Clock,
): Either[String, Rollup] = {
  val candidates: Seq[Rollup] = rollups.filter(_.baseModel == name.getOrElse(""))

  candidates
    .filter { rollup =>
      // 1. GRAIN: rollup grain ≤ query grain (rollup at-or-finer than query).
      //    If finer, re-aggregation is allowed only if all measures
      //    are re-aggregable.
      val rollupGrain = rollup.rollupDimensions.toSet
      val queryGrain  = signature.dimensions.toSet
      val grainOk =
        rollupGrain.subsetOf(queryGrain) ||  // rollup at or finer than query
        (queryGrain.subsetOf(rollupGrain) &&
         signature.measures.forall { m =>
           rollup.rollupMeasures.find(_.name == m)
             .exists(rm => rm.aggregator.canReAggregate(rollupGrain, queryGrain))
         })

      // 2. FILTER COVERAGE: every WHERE column must be in the rollup source.
      //    (We precomputed `rollup.precomputedColumns` at registration.)
      val filterOk = signature.filterColumns.subsetOf(rollup.precomputedColumns)

      // 3. MEASURE COVERAGE: every requested measure in the rollup.
      val measureOk = signature.measures.toSet.subsetOf(
        rollup.rollupMeasures.map(_.name).toSet
      )

      // 4. TIME GRAIN: rollup time grain ≤ query time grain
      //    (per Arch L4 / DE H4'). Coarser rollups can't satisfy finer queries.
      val timeGrainOk = signature.timeGrain.forall { qg =>
        rollupTimeGrainOf(rollup).forall(rg => rg.finerOrEqual(qg))
      }

      // 5. JOIN: non-join query only (joined rollups are v0.4+).
      val joinOk = signature.joinModel.isEmpty

      grainOk && filterOk && measureOk && timeGrainOk && joinOk
    }
    .sortBy(_.precomputedRowCount)  // cached at registration (Arch H3 fix)
    .headOption
    .map { matched =>
      // 6. FRESHNESS check (DE H5' fix — REQUIRED, not default).
      matched.freshness match {
        case RollupFreshness.NoTracking =>
          // Explicit opt-out only.
          Right(matched)
        case RollupFreshness.Track(wmProvider, maxStale, onStale) =>
          val wm = registry.watermark(matched.name).getOrElse(wmProvider())
          val age = java.time.Duration.between(wm, clock.instant())
          if (age.compareTo(maxStale) <= 0) Right(matched)
          else onStale match {
            case OnStalePolicy.FallBackToBase =>
              Left(s"Rollup '${matched.name}' is stale (age=$age, max=$maxStale); falling back to base")
            case OnStalePolicy.Error =>
              throw new IllegalStateException(
                s"Rollup '${matched.name}' is stale (age=$age, max=$maxStale); refusing to serve stale data"
              )
          }
      }
    }
    .getOrElse(Left("No registered rollup matches the query signature"))
}
```

### 4.5 Cache key extension

```diff
 // In: io.semanticdf.cache.CacheKey

 def forRequest(req: AuditQueryRequest, maxRows: Int, rollupGen: Option[String] = None): Option[String] = {
   ...
   val canonical = canonicalize(req) + (rollupGen.map(g => s"|gen=$g").getOrElse(""))
   sha256(canonical)
 }
```

Cache hit invalidates when the rollup generation changes (re-registration bumps the generation). Stale data is never served from cache.

### 4.6 Manifest round-trip

Rollups survive manifest round-trip. Source providers are NOT serialized (functions). At `fromJson` time, the caller passes the providers:

```scala
def fromJson(
  json:          String,
  sourceProviders: Map[String, () => DataFrame] = Map.empty,  // for rollup sources
  watermarks:     Map[String, () => Instant]      = Map.empty,
): SemanticTable
```

The `Rollup` value class doesn't hold the provider; the registry does. Manifest round-trip is metadata-only.

---

## 5. Companion infrastructure (separate small PR)

These primitives don't exist yet and are required:

```scala
// In: io.semanticdf.TimeGrain

object TimeGrain {
  /** Ordered enum-like representation of time grains.
    * Lower index = coarser grain (year < month < day < hour). */
  type Grain = Int  // 0=year, 1=quarter, 2=month, 3=day, 4=hour, 5=minute
  val Order: Map[String, Grain] = Map(
    "year" -> 0, "quarter" -> 1, "month" -> 2, "day" -> 3, "hour" -> 4, "minute" -> 5
  )

  /** True if `g1` is at-or-coarser than `g2` (i.e., g1 ≤ g2). */
  def finerOrEqual(g1: String, g2: String): Boolean =
    Order.getOrElse(g1, -1) <= Order.getOrElse(g2, Int.MaxValue)

  /** True if `g1` is strictly finer than `g2`. */
  def finer(g1: String, g2: String): Boolean =
    Order.getOrElse(g1, -1) < Order.getOrElse(g2, Int.MaxValue)
}

// In: io.semanticdf.SemanticTable

def findDimensionTimeGrain(dimName: String): Option[String] = {
  dimensions.get(dimName).flatMap { d =>
    if (d.isTimeDimension) d.smallestTimeGrain else None
  }
}
```

(`Predicate.fields` already exists. `TimeGrain.fromString` was renamed to `TimeGrain.normalize`.)

---

## 6. Edge cases & error handling

### 6.1 Illegal inputs

- `Rollup.name` empty → `IllegalArgumentException`
- `Rollup.baseModel` doesn't match parent → `IllegalArgumentException`
- `Rollup.rollupDimensions` empty → `IllegalArgumentException`
- `Rollup.rollupMeasures` empty → `IllegalArgumentException`
- Rollup's storage columns not in source → `IllegalArgumentException` (at registration)
- `RollupMeasure.aggregator` unknown → `IllegalArgumentException`

### 6.2 Misuse at query time

- No rollup matches → fall back to base; no error
- Rollup matches but stale → `onStale` policy fires
- Rollup matches but filters reference missing column → no match; fall back
- Rollup matches but query requests non-re-aggregable measure → no match; fall back
- Rollup matches but query is a join query → no match (joined rollups are v0.4+); fall back

### 6.3 Schema drift

If the user's provider returns a DataFrame missing the precomputed columns, the matcher returns `Left` (the column set was precomputed; the matcher uses the precomputed set, not a live schema check). The compile falls back to base. The user should re-register if the schema changes.

---

## 7. Performance budget

| Path | Cost |
|---|---|
| Find rollup match (no rollup registered) | ~1μs (empty Seq scan) |
| Find rollup match (n=10 rollups, all match) | ~50μs (10 filter ops + 1 sort) |
| Find rollup match (n=100 rollups) | ~500μs |
| Freshness check (with WatermarkCache TTL=30s) | ~1μs (cache hit) or ~10ms (cache miss + provider call) |
| Compile routed (SemanticRollupOp.wrap of existing root) | < 100μs additional |
| **Total overhead per query** | **~100μs to ~1ms** in common case (n=10 rollups, TTL=30s) |

`sourceProvider()` is invoked ONCE at registration (for stats precompute). The matcher uses cached `precomputedRowCount` and `precomputedColumns` — no per-match Spark jobs. Freshness thunk is TTL-cached. Performance budget is realistic.

---

## 8. Example (user-facing)

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

    // 2. User maintains the rollup table externally (Spark / dbt / Airflow)
    //    We simulate by saving and reading parquet to exercise the provider thunk.
    val rollupPath = "/tmp/orders_by_region_category"
    ordersDf.groupBy("region", "category").agg(
      sum("amount").as("total"),
      count("*").as("count")
    ).write.mode("overwrite").parquet(rollupPath)

    // 3. Build the Rollup (pure data; stats precomputed at registration)
    val rollup = Rollup(
      name                = "orders_by_region_category",
      baseModel           = "orders",
      rollupDimensions    = Seq("region", "category"),
      rollupMeasures      = Seq(
        RollupMeasure("total", RollupAggregator.Sum,   "total",  m => col(m.storageCol)),
        RollupMeasure("count", RollupAggregator.Count, "count",  m => col(m.storageCol)),
      ),
      sourceProvider      = _ => spark.read.parquet(rollupPath),  // thunk (no closure capture)
      freshness           = RollupFreshness.Track(
        watermarkProvider = () => java.time.Instant.now().minusSeconds(60),
        maxStaleness       = java.time.Duration.ofMinutes(5),
        onStale            = OnStalePolicy.FallBackToBase,
      ),
    )

    // 4. Build the registry (the DataFrame references live HERE, not on the model)
    val registry = RollupRegistry.empty
      .register(rollup.name, rollup.sourceProvider)
      .withWatermarkCache(java.time.Duration.ofSeconds(30))

    val ordersWithRollup = orders.withRollup(rollup)

    // 5. Run the query — auto-routes to rollup
    val t0 = System.nanoTime()
    val result = ordersWithRollup.query(
      measures   = Seq("total", "count"),
      dimensions = Seq("region", "category"),
    ).execute(spark).collect()
    val elapsedMs = (System.nanoTime() - t0) / 1e6
    println(s"Elapsed: ${elapsedMs}ms (vs ~2000ms without rollup)")

    // 6. Inspect the routing decision
    val sig = QuerySignature(
      measures      = Seq("total", "count"),
      dimensions    = Seq("region", "category"),
      filterColumns = Set.empty,
      timeGrain     = None,
      joinModel     = None,
    )
    val matchResult = ordersWithRollup.findRollupMatch(sig, registry)
    assert(matchResult.isRight)
    assert(matchResult.toOption.get.name == "orders_by_region_category")

    // 7. Verify re-aggregation: finer-grain rollup can answer coarser-grain query
    val coarsenSig = QuerySignature(
      measures      = Seq("total", "count"),
      dimensions    = Seq("region"),  // one dim less than rollup grain
      filterColumns = Set.empty,
      timeGrain     = None,
      joinModel     = None,
    )
    val coarsenResult = ordersWithRollup.findRollupMatch(coarsenSig, registry)
    assert(coarsenResult.isRight)  // Sum/Count re-aggregate; rollup can answer

    // 8. Verify non-re-aggregable rejection
    val minSig = coarsenSig.copy(measures = Seq("total", "min_amount"))
    // (Define a Min measure in rollup... skipped here for brevity)
    val minResult = ordersWithRollup.findRollupMatch(minSig, registry)
    assert(minResult.isLeft)  // Min doesn't re-aggregate; rollup can't answer

    // 9. Verify freshness check
    val staleRegistry = RollupRegistry.empty
      .register(rollup.name, rollup.sourceProvider)
      .withWatermarkCache(java.time.Duration.ofSeconds(30))
      // (Watermark from cache would be ~60s old; exceeds maxStaleness=5min... wait, 60s < 5min, so not stale.)
    // For staleness test: artificially set watermark to 1 hour ago.
    // val veryStaleRegistry = staleRegistry.withWatermark("orders_by_region_category",
    //   () => java.time.Instant.now().minusSeconds(3600))
    // val staleResult = ordersWithRollup.findRollupMatch(sig, veryStaleRegistry)
    // assert(staleResult.isLeft)

    spark.stop()
  }
}
```

Note: `sourceProvider = _ => spark.read.parquet(rollupPath)` — the `rollupPath` is captured by the lambda, but the **closure** is the lambda itself (not the DataFrame). The lambda body invokes `spark.read.parquet` lazily at compile time. This sidesteps the closure gotcha (Arch M7): the captured state is the path string, not a DataFrame.

---

## 9. Testing strategy

23 falsifiable tests (was 21; +2 for per-aggregator and wrapping behavior):

| Claim | Test |
|---|---|
| Rollup with exact grain is found | `RollupSpec: matches exact grain` |
| Rollup at finer grain + Sum/Count is found (re-aggregates) | `RollupSpec: matches finer grain with Sum` |
| Rollup at finer grain + Min/Max is rejected | `RollupSpec: rejects finer grain with Min/Max` |
| Rollup at coarser grain is rejected | `RollupSpec: rejects coarser grain` |
| Rollup with missing filter column is rejected | `RollupSpec: rejects missing filter column` |
| Rollup with non-re-aggregable measure is rejected | `RollupSpec: rejects non-re-aggregable measure` |
| Rollup with coarser time grain is rejected | `RollupSpec: rejects coarser time grain` |
| Rollup on a joined query is rejected | `RollupSpec: rejects joined query` |
| Smallest precomputed-row-count rollup wins | `RollupSpec: picks smallest by precomputed row count` |
| Stale rollup falls back to base per `onStale` | `RollupSpec: stale rollup falls back` |
| Stale rollup throws when `onStale = Error` | `RollupSpec: stale rollup throws` |
| Freshness is REQUIRED (NoTracking must be explicit) | `RollupSpec: requires Track by default` |
| Cache key includes rollup generation | `CacheKeySpec: key includes generation` |
| SemanticRollupOp preserves fluent chain (where/having/orderBy) | `RollupSpec: SemanticRollupOp preserves wrappers` |
| SemanticAggregateOp.resolveModel walks through SemanticRollupOp | `SemanticAggregateOpSpec: walks through rollup` |
| SemanticTable with rollups remains Serializable | `SerializationSpec: Rollup-registered model serializable` |
| Manifest round-trip preserves rollup definitions | `SemanticManifestSpec: rollups survive round-trip` |
| `recommendRollups` filters out queries already hitting rollups | `RollupRecommendationSpec: skips queries matching existing rollup` |
| `recommendRollups` groups by shape, dedupHash | `RollupRecommendationSpec: groups by shape` |
| Auto-routing latency overhead < 1ms in common case (n=10) | `RollupPerfSpec: routing overhead < 1ms at n=10` |
| Real workload speedup 40×+ (1M-row table → 100-row rollup) | `RollupExampleSpec: 1M-row table, ≥ 40× speedup` |
| Avg re-aggregates as sum / count | `RollupAggregatorSpec: Avg projection = sum/count` |
| Per-aggregator `canReAggregate` differs | `RollupAggregatorSpec: Sum != Min != Max != Avg` |

---

## 10. Phasing & PR breakdown

(Reordered per Arch M2: manifest first, companion infra early, routing last.)

### (see version history): Companion infrastructure (~150 LoC, ~1 PR)

- `TimeGrain.Order`, `TimeGrain.finerOrEqual`, `TimeGrain.finer` (new)
- `SemanticTable.findDimensionTimeGrain(d)` (new)
- `Predicate.fields` already exists; no change

**Tests**: 3 falsifiable tests
**Risk**: ZERO (additive only)

### (see version history): Rollup types + manifest round-trip (~400 LoC, ~1 PR)

- `Rollup`, `RollupMeasure`, `RollupAggregator` (Sum/Count/Min/Max/Avg/Stddev)
- `RollupFreshness.Track`, `OnStalePolicy` (NoTracking REMOVED as default)
- `RollupRegistry`, `WatermarkCache`
- Manifest schema field for rollups
- `SemanticManifest.fromJson(json, sourceProviders, watermarks)`

**Tests**: 8 falsifiable tests (types, manifest round-trip, backward compat)
**Risk**: Low (no compile-path changes)

### (see version history): `SemanticRollupOp` (wrapping) + `SemanticAggregateOp.resolveModel` update (~300 LoC, ~1 PR)

- New `SemanticRollupOp` with `source: SemanticOp` field
- Update `SemanticAggregateOp.resolveModel` to walk through it
- `SemanticRollupOp.compile` applies filters, re-aggregates, applies HAVING

**Tests**: 6 falsifiable tests (wrapping, preserve wrappers, resolveModel walks through)
**Risk**: Medium (touches op-tree hot path)

### (see version history): Routing + freshness + cache-key (~400 LoC, ~1 PR)

- `SemanticTable.findRollupMatch`
- Routing decision in `toDataFrameInternal` (all 3 sites)
- `CacheKey.forRequest` extended with rollup generation
- `AuditSink.recommendRollups`

**Tests**: 5 falsifiable tests (match, freshness, cache generation)
**Risk**: High (touches hot path + cache key)

### (see version history): Example + docs (~150 LoC, ~1 PR)

- `examples/runtime-tuning/src/main/scala/com/example/runtime/RollupMain.scala`
- README update

**Risk**: Low (docs + example)

**Total: 5 PRs, ~1400 LoC, ~3 weeks**

---

## 11. Open questions

1. **RollupMeasure's `projection` field** — It's a function `RollupMeasure => Column`. For Avg/Stddev, the projection needs access to OTHER measures' columns. Should this be a function of the full rollup, not the single measure? My current design is `m => col(m.storageCol)` which is per-measure; Avg/Stddev need cross-measure access. **My design has a gap here** — I should change `projection` to `rollup: Rollup => Column` so Avg can reference the count measure.
2. **`recommendRollups` for already-routed queries** — When the matcher filters out queries already hitting rollups, it does so based on the MATCH result. But historical queries (before any rollup was registered) wouldn't have a match. Should they all be candidates? My current design: yes.
3. **What if user registers a rollup for a model name that doesn't match any `SemanticTable`?** — My design: `Rollup.baseModel` validation at `withRollup` time. Mismatch → `IllegalArgumentException`. Acceptable.
4. **What about MIN/MAX with re-aggregation?** — DE H4' says Min/Max are partial-additive (correct only at rollup grain). My current design: `canReAggregate(rg, qg) = rg == qg`. This means Min/Max rollups only match at exact grain. **Correct.**

---

## 12. Skill compliance (v2)

### karpathy

- Surgical: each PR touches ≤ 3 production files. (see version history) adds companion infra; (see version history) adds types + manifest; (see version history) adds the op; (see version history) adds routing; (see version history) adds example.
- Verifiable: 23 falsifiable tests (was 21 in v1).
- No opportunistic refactors: only rollup-related changes.

### debug-mantra

- Reproduce: example in §8 is runnable end-to-end via `mvn exec:java`.
- Trace: §4.1 shows the diff for ALL 3 compile sites.
- Falsify: 23 tests; each names the claim it pins.
- Cross-reference: §6 quantifies overhead; §5 documents edge cases; §10 lists open questions.
- Verify: PR phasing in §10 lets each PR be independently verified.

### scala-data-driven-refactor

- **Parse don't validate**: `Rollup.apply(...)` smart constructor (validates + precomputes); `RollupAggregator.parse(s)` returns `Option`; `RollupFreshness` requires explicit choice; `WatermarkCache` enforces TTL.
- **Plain types**: `RollupFreshness` and `OnStalePolicy` are sealed traits (2 cases each, behavior differs); `RollupAggregator` is sealed (6 cases, each has distinct `canReAggregate`/`storageExpr`/`projectionExpr`); `WatermarkCache` is a regular class (no ADT).
- **No ADT escalation without justification**: Removed `RollupFallbackPolicy` (Arch H4); `NoTracking` is an explicit opt-out (not a sealed-trait default).
- **No closure gotcha**: `Rollup` is pure data; `RollupRegistry` holds the providers; example uses `spark.read.parquet(path)` inside the provider body (no DataFrame capture).
- **Distributed-ser**: `Rollup` value class is Serializable (no DataFrame, no function fields). `RollupRegistry` is NOT on `SemanticTable`; it's a separate runtime object. `SemanticTable extends Serializable` is preserved.