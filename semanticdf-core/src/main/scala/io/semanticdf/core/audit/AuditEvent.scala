package io.semanticdf.core.audit

import java.time.Instant

import io.semanticdf.core.engine.EngineIdentity

/** Engine-portable mirror of `io.semanticdf.audit.AuditEvent` —
  * Phase 1 increment 9: data-only audit event for engine adapters.
  *
  * Mirrors `io.semanticdf.audit.AuditEvent` with ONLY the data parts
  * and the pure `dedupHashOf` factory. No Spark references, no
  * `AuditSink` coupling (that's a behavior trait, lives in the
  * spark-bearing package).
  *
  * ==Why this exists==
  *
  * Future engine adapters (Trino, Databricks, custom-platform) need
  * to emit audit events when their queries run. They can do this
  * without depending on Spark by using this core mirror — they
  * construct the data record directly, and the platform's
  * `AuditService` (Java/Postgres side) consumes it via the
  * `Restate` journal.
  *
  * ==Data model (parse-don't-validate, per `scala-data-driven-refactor` step 1-2)==
  *
  * Every field carries exactly what its semantics require:
  *   - `ts`           — wall-clock time the query started (`Instant`).
  *   - `model`        — the `SemanticTable`'s name.
  *   - `version`      — model version at the time this query ran.
  *   - `measures`     — names of measures the caller asked for.
  *   - `dimensions`   — names of dimensions the caller asked for.
  *   - `whereHash`    — stable hash of the `where` predicate tree.
  *   - `havingHash`   — same, for the `having` predicate.
  *   - `rowCount`     — number of rows in the result (after any limit).
  *   - `elapsedMs`    — wall-clock time from query start to result.
  *   - `status`       — "ok" or "error".
  *   - `error`        — error class + message, populated when error.
  *   - `requester`    — optional free-form label for the caller.
  *   - `requestId`    — optional per-call id (e.g. a UUID).
  *   - `dedupHash`    — replay-safe dedup key (see `dedupHashOf`).
  *   - `executedPlan` — engine-specific plan (None for non-Spark engines).
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` with 15 fields, no behavior methods.
  * - Equality auto-derived (case class).
  * - Hash codes stable (auto-derived).
  * - `dedupHashOf` is a pure function over data (no I/O, no clock).
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports. Verifiable by:
  * `grep 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/audit/AuditEvent.scala`
  *
  * The original `io.semanticdf.audit.AuditEvent` remains the canonical
  * Spark-bearing type (the library's fluent API emits it). This core
  * mirror is additive — engine-portable consumers (future Trino,
  * Databricks adapters) can depend on this without dragging Spark.
  *
  * ==Consolidation plan (NOT in this PR)==
  *
  * Phase 2: convert the library to emit core.AuditEvent from the
  * fluent API. After that, the original `io.semanticdf.audit.AuditEvent`
  * becomes a type alias (or is removed) and consumers stop importing
  * two parallel types.
  */
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
    /** Engine-specific executed plan (None for engines that don't
      * surface an executed plan; Spark populates this with
      * `df.queryExecution.executedPlan.toString()`). */
    executedPlan: Option[String] = None,
    /** Engine identity (per design §4.5.5). Used by the dedup
      * key so a Spark request and a Trino request for the same
      * model produce DIFFERENT audit events (per round-3 DE
      * finding 11 closure). `None` for events written before
      * this field was added — old events read as `None`, new
      * events write `Some(...)`. */
    engine:       Option[EngineIdentity] = None,
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
      engine:     Option[EngineIdentity] = None,
  ): String = {
    import java.security.MessageDigest
    val engineStr = engine.map { e =>
      s"${e.name}|${e.nativeVersion}|${e.engineAdapterVersion}"
    }.getOrElse("")
    val canonical = s"${model}|${version}|" +
      s"${measures.sorted.mkString(",")}|" +
      s"${dimensions.sorted.mkString(",")}|" +
      s"${whereHash.getOrElse("")}|" +
      s"${havingHash.getOrElse("")}|" +
      s"${engineStr}"
    val bytes = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes("UTF-8"))
    bytes.map(b => f"${b & 0xff}%02x").mkString
  }
}