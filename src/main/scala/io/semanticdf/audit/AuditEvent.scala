package io.semanticdf.audit

import java.time.Instant

/** One entry in the query audit log — emitted by [[SemanticTable.toDataFrame]]
  * (and by MCP's `query` tool) every time a semantic query executes.
  *
  * == Why ==
  *
  * LLM agents running on top of the semantic layer make queries that humans
  * don't review line by line. An audit log is the only way to answer:
  *
  *   - What did my agent just query?
  *   - Is the agent making the same query repeatedly (cache candidate)?
  *   - Did the last agent run hit a timeout or error?
  *   - How long did the query take, and how many rows came back?
  *
  * == Field semantics ==
  *
  *   - `ts`           — wall-clock time the query started.
  *   - `model`        — the [[io.semanticdf.SemanticTable]]'s name (or
  *                      `sourceTable` if no name is set).
  *   - `measures`     — names of measures the caller asked for.
  *   - `dimensions`   — names of dimensions the caller asked for.
  *   - `whereHash`    — stable hash of the `where` predicate tree, or
  *                      None if no filter. Two equivalent ASTs hash to
  *                      the same value. Use this for cache-key
  *                      equivalence.
  *   - `havingHash`   — same, for the `having` predicate.
  *   - `rowCount`     — number of rows in the result (after any limit).
  *   - `elapsedMs`    — wall-clock time from `toDataFrame` start to
  *                      result collection.
  *   - `status`       — `"ok"` or `"error"`.
  *   - `error`        — error class + message, populated when
  *                      `status == "error"`.
  *   - `requester`    — optional free-form label for the caller
  *                      (e.g. an agent id, a session id). Default `None`.
  *   - `requestId`    — optional per-call id (e.g. a UUID). Default
  *                      `None`; the MCP layer populates this so a
  *                      single MCP tool call's `query` + its retries
  *                      can be correlated.
  *
  * == Hashing ==
  *
  * `whereHash` and `havingHash` are produced by [[PredicateHasher]]. The
  * hasher walks the `Predicate` tree and emits a stable canonical string,
  * then SHA-256s it. The wire AST and the library `Predicate` form the
  * same hash when they describe the same filter — that is the contract
  * we need to make caching by AST equivalence possible later. */
final case class AuditEvent(
    ts:           Instant,
    model:        String,
    measures:     Seq[String],
    dimensions:   Seq[String],
    whereHash:    Option[String],
    havingHash:   Option[String],
    rowCount:     Long,
    elapsedMs:    Long,
    status:       String,        // "ok" | "error"
    error:        Option[String] = None,
    requester:    Option[String] = None,
    requestId:    Option[String] = None,
)
