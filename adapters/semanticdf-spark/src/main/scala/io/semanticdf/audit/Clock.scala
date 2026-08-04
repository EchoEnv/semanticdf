package io.semanticdf.audit

/** A clock for replay-safe auditing. The `clock: () => Instant` is the
  * dependency-injection seam for the platform's Restate replay-stable
  * time. Library callers that don't pass a `clock` get the system
  * default (NOT replay-stable) — the default is the backward-compat
  * guarantee; the platform thread is what makes the audit path
  * replay-safe.
  *
  * == Why a function (not a class / trait) ==
  *
  * A function is the simplest dependency-injection seam. It costs
  * one allocation per call site that uses it, has no OOP ceremony,
  * and matches the existing implicit-parameter style of the library
  * (e.g. `toDataFrame(implicit spark: SparkSession)` at
  * `SemanticTableCore.scala:75`). A `Clock` trait would add a class
  * without buying anything; a builder pattern would break the atomic
  * compile + emit-audit-in-same-call contract.
  *
  * == How the platform uses it ==
  *
  * In a Restate handler:
  * {{{
  *   val clock = () => Restate.instantNow()
  *   val st    = model.copy(...).toDataFrame()(implicit spark, clock)
  *   val json  = SemanticManifest.toJson(model)(implicit clock)
  * }}}
  *
  * The thunk `() => Restate.instantNow()` is REPLAY-STABLE: the same
  * value is returned during original execution and journal replay.
  * All `ts` and `compiledAt` fields in the resulting AuditEvents and
  * manifest JSONs are equal to the original. Without the platform
  * supplying this thunk, the default `Clock.systemDefault` is
  * wall-clock-of-write and replay-replay drift is possible. */
object Clock {

  /** The system default: wall-clock of the call site. NOT replay-safe.
    * Library callers that don't supply an explicit `clock` get this.
    * Platform callers on the Restate boundary MUST pass
    * `() => Restate.instantNow()` instead. */
  val systemDefault: () => java.time.Instant = () => java.time.Instant.now()
}
