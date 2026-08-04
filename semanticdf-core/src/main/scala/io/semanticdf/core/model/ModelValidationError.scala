package io.semanticdf.core.model

/** Engine-portable model-validation-error ADT — Phase 2 contract.
  * Mirrors the design doc §4.4.1 "ModelValidationError" (5 cases:
  * InvalidName, DuplicateMember, UnknownReference,
  * CalcDepthExceeded, ExtensionEnvelopeExceeded).
  *
  * [[ModelValidationError]] is the structured error returned by
  * [[Model.of]] when validation fails. It's an ADT (not a Throwable)
  * — the caller pattern-matches to render a user-facing message.
  *
  * ==Why a sealed ADT (vs. a String or a Throwable)==
  *
  * A `String` would let callers pass any string — no compile-time
  * guarantee that every error case is handled at the MCP boundary.
  * A `Throwable` would force callers to `catch` and lose the
  * structured fields. The sealed ADT carries the structured fields
  * (the name, the depth, the byte count, etc.) and forces the
  * caller to handle every case explicitly via pattern match.
  *
  * Per scala-data-driven-refactor §3 ("A rule becomes data only
  * when it must change without a deploy"): the error case set is
  * fixed at compile time (5 cases, defined by the design). A sealed
  * ADT is correct, NOT a Map.
  *
  * ==Why 5 cases (not fewer, not more)==
  *
  * The set covers the validation errors that [[ModelValidator]]
  * checks:
  *   - **InvalidName** (check 1): the model's name is blank or empty
  *   - **DuplicateMember** (check 2): dimension/measure/calc-measure
  *     names collide
  *   - **UnknownReference** (check 3): a calc-measure references a
  *     measure that wasn't declared
  *   - **CalcDepthExceeded** (check 4): the calc-measure DAG exceeds
  *     the depth cap (engine-specific, passed at compile time)
  *   - **ExtensionEnvelopeExceeded** (check 5): the inline extension
  *     envelope exceeds the size or field limits
  *
  * Policy defaults (check 6) are well-formed by construction (the
  * ADTs are closed), so no error case is needed for them.
  *
  * ==Why core (engine-portable)==
  *
  * Validation errors are universal — every engine that builds
  * portable models produces the same error shape. The engine-
  * specific rendering (Spark's `IllegalArgumentException` with
  * a stack trace, Trino's error JSON, Databricks' REST error
  * envelope) lives in the engine adapter's compile step.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 5 case classes
  * - Equality auto-derived (case classes)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/ModelValidationError.scala`
  */
sealed trait ModelValidationError extends Product with Serializable

object ModelValidationError {

  /** Check 1: the model's name is blank or empty. `reason` is
    * the human-readable explanation ("name is blank", etc.).
    * Maps to MCP's `ErrorDetail("invalid_name", reason)`. */
  final case class InvalidName(reason: String) extends ModelValidationError

  /** Check 2: a dimension/measure/calc-measure name collides with
    * another member. `kind` is "dimension", "measure", or
    * "calculatedMeasure". `name` is the colliding member name.
    * Maps to MCP's `ErrorDetail("duplicate_member", s"$kind: $name")`. */
  final case class DuplicateMember(kind: String, name: String)
      extends ModelValidationError

  /** Check 3: a calc-measure references a measure that wasn't
    * declared in the model. `referent` is the field/member that
    * carries the reference (e.g. "calculatedMeasures"); `target`
    * is the unresolved name.
    * Maps to MCP's `ErrorDetail("unknown_reference", s"$referent: $target")`. */
  final case class UnknownReference(referent: String, target: String)
      extends ModelValidationError

  /** Check 4: the calc-measure DAG exceeds the depth cap. `depth`
    * is the actual depth; `max` is the engine's capability cap.
    * Maps to MCP's `ErrorDetail("calc_depth_exceeded", s"$depth > $max")`. */
  final case class CalcDepthExceeded(depth: Int, max: Int)
      extends ModelValidationError

  /** Check 5: the inline extension envelope exceeds the size or
    * field limits. `fieldCount` is the actual recursive field
    * count; `byteCount` is the actual byte count (canonical UTF-8).
    * Maps to MCP's `ErrorDetail("extension_envelope_exceeded", ...)`. */
  final case class ExtensionEnvelopeExceeded(fieldCount: Int, byteCount: Int)
      extends ModelValidationError
}