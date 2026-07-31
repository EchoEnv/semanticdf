package io.semanticdf
import io.semanticdf.predicate._

import io.semanticdf.audit.{AuditEvent, AuditSink, QueryRequest => AuditQueryRequest}

import org.apache.spark.sql.{Column, Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import scala.jdk.CollectionConverters._

/** Immutable facade over the root of a semantic op tree (DESIGN §4.1).
  *
  * A `SemanticTable` is *not* a Spark `DataFrame`; it is a deferred, source-agnostic
  * definition that compiles to a DataFrame at an execution terminal. The batch terminal
  * is [[SemanticTable.toDataFrame]] / [[SemanticTable.execute]]; the streaming
  * terminal is [[SemanticTable.toStreamingQuery]]. Same definition, different sink,
  * mirroring Spark's own `df.write` vs `df.writeStream`.
  */
final class SemanticTable private[semanticdf] (
    private[semanticdf] val root: SemanticOp,
    private[semanticdf] val postAggPredicates: List[Predicate] = Nil,
    /** Per-model schema version, propagated to MCP/OKF consumers.
      *
      * `0` means "pre-versioning era" — the model declaration did not commit to
      * a version. The library never fails on a mismatch; it just stores and emits
      * the value. Compatibility policy is the consumer's problem (MCP server,
      * agent framework, downstream pipelines).
      *
      * Defaults to 0. Set via the YAML `version:` field or the fluent `.version(n)` setter.
      */
    val version: Int = 0,
    /** Name of the underlying source DataFrame this model was built from, if known.
      *
      * Populated by [[YamlLoader]] from the YAML `table:` field — the name used
      * to resolve the source DataFrame against either a caller-supplied map or
      * the Spark catalog. Unset (None) for models built directly from the Scala
      * DSL ([[io.semanticdf.toSemanticTable]]) where there's no equivalent concept.
      *
      * Used by MCP `describe_model` to expose the origin of a model's data to
      * consumers (LLM agents, BI tools, lineage trackers).
      */
    val sourceTable: Option[String] = None,
    /** Lifecycle status of this model. Surfaced by MCP `describe_model`,
      * the manifest artifact (`SemanticManifest.toJson`), and OKF generation
      * so consumers (LLM agents, BI tools, downstream pipelines) can decide
      * whether to query, warn, or refuse.
      *
      * Defaults to [[ModelStatus.Published]] for backwards compatibility —
      * models built in v0.1.x implicitly were published; carrying that
      * semantics forward keeps existing programs working without change.
      * New models can declare `status: draft` / `published` / `deprecated`
      * in YAML or via the fluent setter [[status(s:ModelStatus)* status]].
      *
      * Lifecycle is purely informational at the library level — the query
      * terminals (`toDataFrame`, `toStreamingQuery`, `execute`) do not
      * consult status. Consumers enforce policy.
      */
    val status: ModelStatus = ModelStatus.Published,
    /** Audit log sink — when set, every `toDataFrame` / `execute` call
      * that traces back to a `query()` invocation emits an
      * [[io.semanticdf.audit.AuditEvent]] describing the request
      * shape and the execution result. Default `None` (no audit) so
      * the audit path is opt-in.
      *
      * Set via the fluent `.withAuditSink(sink)` setter. Survives the
      * fluent chain (`.query(...).limit(...).toDataFrame(...)` keeps
      * the sink) so a single setter call at the model level covers
      * every downstream query.
      *
      * Join-construction propagation (post-#307 audit M3): when the
      * outer table is constructed by `join_one` / `join_many` /
      * `join_cross`, the outer's `auditSink` is
      * `orElse(this.auditSink, other.auditSink)` — LEFT wins when both
      * sides set a sink; RIGHT is the fallback so a user who set
      * the sink on the right side (intuitively: "audit this join
      * result") gets the sink. Implementation: `joinAuditSink`
      * helper in `SemanticTableMutation`. */
    val auditSink: Option[io.semanticdf.audit.AuditSink] = None,
    /** Captured request shape for audit emission. Populated by
      * [[query]] (and the streaming variants); preserved across the
      * fluent chain so the audit event carries the user's original
      * request, not the post-chain op tree.
      *
      * Default `None`. When `auditSink` is also `None`, this field
      * is dormant — no hashing cost.
      *
      * Join-construction propagation (post-#307 audit M3): when the
      * outer table is constructed by `join_one` / `join_many` /
      * `join_cross`, the outer's `auditRequest` is
      * `orElse(this.auditRequest, other.auditRequest)` — LEFT wins
      * when both sides set a request; RIGHT is the fallback. The
      * captured request is needed to compute the cache key
      * (see [[io.semanticdf.cache.CacheKey.forRequest]]), so the
      * propagation rule matches `resultCache` (they're set/cleared
      * together). Implementation: `joinAuditRequest` helper in
      * `SemanticTableMutation`. */
    val auditRequest: Option[AuditQueryRequest] = None,
    /** Result cache — when set, every `toDataFrame` / `execute` call
      * that traces back to a `query()` invocation checks the cache
      * first (by a stable SHA-256 of the request shape) and returns
      * the cached rows on hit. On miss, the result is stored before
      * the DataFrame is returned. Default `None` (no cache) so the
      * cache path is opt-in.
      *
      * The cache key is derived from `auditRequest`, so the
      * fluent chain must capture the request via `query(...)` for
      * caching to work — directly-built chains (`groupBy(...).aggregate(...)`)
      * bypass the cache.
      *
      * Join-construction propagation (post-#307 audit M3): when the
      * outer table is constructed by `join_one` / `join_many` /
      * `join_cross`, the outer's `resultCache` is
      * `orElse(this.resultCache, other.resultCache)` — LEFT wins
      * when both sides set a cache; RIGHT is the fallback so a
      * user who set the cache on the right side (intuitively:
      * "cache the join result") gets the cache. Note that
      * `auditRequest` follows the same `orElse` rule (they're
      * set/cleared together). Implementation: `joinResultCache`
      * helper in `SemanticTableMutation`. */
    val resultCache: Option[io.semanticdf.cache.ResultCache] = None,
    /** Driver-memory safety cap on the rows returned by the cache miss path.
      *
      * On a cache miss, `toDataFrame` calls `df.limit(maxRows).collect()` —
      * the materialised row array is the dominant memory cost of the cache.
      * Without this cap, a 10M-row query OOMs the driver. The default mirrors
      * the canonical `CacheKey.DefaultMaxRows` (100,000) so library and
      * platform agree on the safety threshold.
      *
      * `maxRows > 0`  → apply `df.limit(maxRows)` before `collect()`.
      * `maxRows == 0` → no cap (escape hatch; not recommended).
      *
      * The cap applies on every path that materialises rows on the
      * caller's behalf: the cache-miss path AND the audit-only
      * no-cache-key path (`auditSink=Some, resultCache=None`,
      * `auditRequest=Some`). The audit-only path is no longer lazy
      * (it collects capped rows so the emitted `AuditEvent.rowCount`
      * is accurate rather than 0).
      *
      * The cap does NOT apply on:
      *   - the no-cache fast path (`auditSink.isEmpty && resultCache.isEmpty`),
      *     which returns a lazy DataFrame and lets the caller drive `collect()`
      *   - the cache-hit path, where the producer already bounded the cached
      *     row array
      *
      * Set via the fluent `.withMaxRows(n)` setter. Survives the fluent
      * chain the same way `resultCache` does. Manifest round-trip:
      * non-default values are emitted under `runtime.maxRows` and
      * restored on `fromJson` (PR #303). The `maxRows = 0` escape
      * hatch is preserved (not silently coerced to default).
      *
      * Join-construction propagation (post-#307 audit M3): when the
      * outer table is constructed by `join_one` / `join_many` /
      * `join_cross`, the outer's `maxRows` is
      * `min(this.maxRows, other.maxRows)` — the tighter cap wins.
      * The `0 = no-cap` sentinel propagates correctly because
      * `min(100_000, 0) = 0` (a user who disabled the cap on
      * either side gets a cap-less join result). Implementation:
      * `joinMaxRows` helper in `SemanticTableMutation`. */
    val maxRows: Int = io.semanticdf.cache.CacheKey.DefaultMaxRows,
    /** Opt-in auto-broadcast threshold for joins (size-based, bytes).
      *
      * When `Some(n)`, the equi-join compile path queries
      * `df.queryExecution.optimizedPlan.stats.sizeInBytes` on the right
      * side of every `join_one` / `join_many` and applies
      * `broadcast(right)` if that side is smaller than `n`. This
      * overrides Spark's `autoBroadcastJoinThreshold` cost-based
      * decision for this specific query.
      *
      * For `join_many`, the right side passed to the broadcast check
      * is the BSL safe-aggregation of the original right side at the
      * join-grain keys (see `preAggregateAtGrain`). That's typically
      * much smaller than the raw right side — set the threshold
      * against the post-aggregation estimate, not the raw table size.
      *
      * `None` (the default) means "no library override — let Spark
      * decide" — the existing `autoBroadcastJoinThreshold` (default
      * 10MB) still applies.
      *
      * Escape hatch: the builder accepts `bytes = 0` and converts it
      * to `None` here (rather than `Some(0)`). The two values are
      * semantically identical: "no override". This mirrors the
      * `maxRows == 0` convention.
      *
      * Opt-in by design: forcing broadcast can backfire on large
      * sides (driver OOM). Users opt in only when they KNOW the right
      * side is small (dimension table, lookup table, etc.).
      *
      * Note: cross joins (`join_cross`) and the streaming foreachBatch
      * path don't honour this threshold — the broadcast hint is only
      * emitted in `compileEquiJoin` (used by `join_one` / `join_many`).
      *
      * Join-construction propagation rule (PR #306): when the
      * threshold is set on EITHER side of a `join_one` / `join_many`,
      * the op's `broadcastJoinThreshold` is populated via
      * `this.broadcastJoinThreshold.orElse(other.broadcastJoinThreshold)`.
      * Precedence: LEFT wins when both sides carry a threshold; RIGHT
      * is the fallback so a user who set the threshold on the right
      * side (intuitively: "I know this dimension table is small")
      * gets the broadcast hint. The same `orElse` rule applies to
      * the joined-manifest reader (PR #307) so round-trip preserves
      * the threshold regardless of which side carried it.
      *
      * Set via the fluent `.withBroadcastJoinThreshold(bytes)` setter.
      * Survives the fluent chain the same way `resultCache` does.
      * Manifest round-trip preserves the threshold: a non-default
      * value is emitted under `runtime.broadcastJoinThreshold` and
      * restored on `fromJson` (PR #303). Missing/absent means
      * `None` (the library default). */
    val broadcastJoinThreshold: Option[Long] = None,
    /** Opt-in DataFrame persistence (a.k.a. "materialize" — pre-collect
      * caching). When `Some(level)`, the fast path of `toDataFrame`
      * (the `auditSink.isEmpty && resultCache.isEmpty` branch) calls
      * `df.persist(level)` on the compiled DataFrame before returning
      * it, so subsequent actions on the returned `DataFrame` reuse the
      * persisted storage instead of re-executing the Spark plan.
      *
      * The audit/cache branch (when `auditSink` or `resultCache` is
      * set) does NOT honour this flag — the row cache already
      * returns a `parallelize`-based DataFrame that's effectively
      * `MEMORY_ONLY` for the duration of the call, so re-running the
      * Spark plan would be redundant. Applying `persist` to the
      * compiled DataFrame in the audit/cache branch would also leak
      * cluster storage (the user never sees the compiled DataFrame,
      * only the `parallelize` rebuild), so it's deliberately
      * suppressed there. Set via the fluent `.withMaterialize(level)`
      * setter.
      *
      * Lifecycle (this is the whole reason this setter exists):
      * `df.persist(level)` is a marker; the persist happens on the
      * next action. The library does NOT retain a `DataFrame`
      * reference on the table (deliberately — see the design doc for
      * the multi-thread / leak analysis), so `unpersist()` is the
      * caller's responsibility: call `df.unpersist()` on the
      * `DataFrame` you got from `toDataFrame()`. The library does not
      * expose an `unpersist()` method on `SemanticTable` precisely
      * to avoid the volatile-ref race + cluster memory leak.
      *
      * Streaming path: `batchModel` (in `SemanticTableStreaming`)
      * constructs the per-micro-batch `SemanticTable` via named
      * args that don't include `materializeLevel`, so the field
      * defaults to `None` on each batch and persist does NOT fire
      * per micro-batch. Persisting per micro-batch would be
      * meaningless (each batch's DataFrame is consumed once via
      * `foreachBatch`).
      *
      * Sentinel: `None` (the default) means "no persist". The
      * library does not pre-curate a subset of Spark's storage
      * levels — the full `org.apache.spark.storage.StorageLevel`
      * enum is the API surface. Operators who know Spark already
      * know the enum. Storage level choice is the operator's
      * responsibility: `MEMORY_ONLY` on a 10M-row query can OOM the
      * cluster; the library can't paper over that.
      *
      * Join-construction propagation (post-#309 audit pattern):
      * when the outer table is constructed by `join_one` /
      * `join_many` / `join_cross`, the outer's `materializeLevel` is
      * `orElse(this.materializeLevel, other.materializeLevel)` —
      * LEFT wins when both sides set a level; RIGHT is the fallback
      * so a user who set the level on the right side (intuitively:
      * "I want the join result persisted") gets it. Implementation:
      * `joinMaterializeLevel` helper in `SemanticTableMutation`. */
    val materializeLevel: Option[org.apache.spark.storage.StorageLevel] = None,
) extends Serializable with SemanticTableCore with SemanticTableStreaming with SemanticTableMutation with SemanticTableCollection {
}
