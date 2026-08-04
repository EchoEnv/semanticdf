package io.semanticdf

/** Structured result of a [[SemanticTable.validate]] call.
  *
  * - `errors`   are conditions that would cause `execute()` to throw at runtime.
  * - `warnings` are conditions that are legal but worth surfacing (e.g. a time
  *              dimension with no `smallestTimeGrain` would surprise `atTimeGrain()`).
  *
  * `isValid` is the boolean summary; CI checks use that directly. */
final case class ValidationResult(
    errors: Seq[String],
    warnings: Seq[String],
) {
  def isValid: Boolean = errors.isEmpty
  def hasIssues: Boolean = errors.nonEmpty || warnings.nonEmpty
}
