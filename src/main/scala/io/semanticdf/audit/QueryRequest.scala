package io.semanticdf.audit

import io.semanticdf.predicate.Predicate

/** Audit-side capture of a user's query request.
  *
  * Populated by [[io.semanticdf.SemanticTable.query]] (and the streaming
  * variants) and preserved across the fluent chain so the audit event
  * carries the user's original intent — not the post-chain op tree.
  *
  * Field set is intentionally minimal: only what the audit event needs.
  * Order, limit, time grain, and time range are NOT in the audit
  * payload itself (the audit event records what ran, not the
  * shape), BUT they ARE in this DTO because the cache key is
  * derived from it. Two queries that differ only in time grain
  * return different rows, so the cache must distinguish them.
  *
  * Lives in the `audit` package so the library core stays small.
  * The library depends on `audit`; `audit` does not depend on the
  * library's internals. */
final case class QueryRequest(
    model:      String,
    /** Model version at query time, propagated from [[io.semanticdf.SemanticTable.version]].
      *
      * Carried in the captured request so the cache key naturally differentiates
      * entries across model versions: a version bump produces a different
      * cache key, so old entries become unreachable and LRU evicts them. This
      * is the auto-invalidation hook recommended by the v0.1.17 review — the
      * model's `version` is the canonical "upstream changed" signal.
      *
      * Defaults to `0` (the pre-versioning-era sentinel from
      * [[io.semanticdf.SemanticTable.version]]'s contract). When v0.2.0+ picks up
      * versions from YAML `version:` fields, this field captures the live value
      * at the time the user's `query()` built the request. */
    version:    Int = 0,
    measures:   Seq[String] = Seq.empty,
    dimensions: Seq[String] = Seq.empty,
    where:      Option[Predicate] = None,
    having:     Option[Predicate] = None,
    /** Sort spec, captured for cache-key equivalence. The order
      * (and direction) of the keys is part of the result contract:
      * `Seq(carrier asc, revenue desc)` is NOT the same answer as
      * `Seq(carrier desc, revenue asc)`. */
    orderBy:    Seq[(String, String)] = Seq.empty,
    /** Top-N cap. `None` (no cap) and `Some(10)` are different
      * answers, so the key distinguishes them. */
    limit:      Option[Int] = None,
    /** Single time-grain applied to all time dimensions. Empty when
      * the caller didn't specify one. */
    timeGrain:  Option[String] = None,
    /** Per-dimension time-grain map. Empty when the caller didn't
      * specify per-dimension grains. */
    timeGrains: Map[String, String] = Map.empty,
    /** Half-open `[start, end]` range filter. `None` means no
      * range filter. */
    timeRange:  Option[(String, String)] = None,
)
