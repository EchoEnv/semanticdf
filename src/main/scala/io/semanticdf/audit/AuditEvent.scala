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
  * we need to make caching by AST equivalence possible later.
  *
  * == Replay-safe `dedupHash` ==
  *
  * `dedupHash` is the contract gate for the platform's `Restate` replay
  * dedup (per `docs/design/platform-determinism-audit.md`). It is a
  * SHA-256 over the **query-shape fields only** — `model`, `version`,
  * `measures`, `dimensions`, `whereHash`, `havingHash`. It deliberately
  * EXCLUDES `ts`, `elapsedMs`, `rowCount`, `status`, `error`,
  * `requester`, `requestId` — those are non-deterministic (wall-clock,
  * runtime-measured, request-scoped) and including them would defeat
  * the dedup. On Restate replay, the same query produces the same
  * `dedupHash` → the platform's `AuditService.append` no-ops the
  * duplicate.
  *
  * Use [[AuditEvent.dedupHashOf]] to compute it. */
final case class AuditEvent(
    ts:           Instant,
    model:        String,
    /** Model version at the time this query ran. Recorded so consumers
      * (MCP, dashboards, agent frameworks) can correlate audit events
      * with the model state — "did this query run against v1 or v2?"
      * Defaults to `0` (pre-versioning). */
    version:      Int = 0,
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
    /** Replay-safe dedup key — see class doc. Must be set by the call
      * site (no default) so the contract is "always set." Use
      * [[AuditEvent.dedupHashOf]] to compute. */
    dedupHash:    String,
)

object AuditEvent {

  /** Compute the replay-safe `dedupHash` from the 6 query-shape
    * fields. SHA-256 over a canonical pipe-separated string. The
    * `measures` and `dimensions` lists are sorted before hashing so
    * `Seq("a","b")` and `Seq("b","a")` produce the same hash (matches
    * the existing `PredicateHasher` commutative contract for `And`/`Or`).
    *
    * This is a pure function of its inputs — no I/O, no clock. Two
    * calls with the same arguments return equal strings. Restate's
    * `instantNow()` does NOT appear here; the audit's wall-clock
    * timestamp is in `AuditEvent.ts`, which is intentionally NOT
    * part of the dedup key. */
  def dedupHashOf(
      model:      String,
      version:    Int,
      measures:   Seq[String],
      dimensions: Seq[String],
      whereHash:  Option[String],
      havingHash: Option[String],
  ): String = {
    import java.security.MessageDigest
    val canonical = s"${model}|${version}|" +
      s"${measures.sorted.mkString(",")}|" +
      s"${dimensions.sorted.mkString(",")}|" +
      s"${whereHash.getOrElse("")}|" +
      s"${havingHash.getOrElse("")}"
    val bytes = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes("UTF-8"))
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }
}
