package io.semanticdf.core.catalog

/** Engine-portable publication-result ADT \u2014 PR 10 of the v0.3.0
  * deferred-work triage.
  *
  * Returns from [CatalogAdapter.publish]. The closed ADT forces
  * every adapter to surface the result shape explicitly (no
  * exception-based reporting for normal flow control).
  *
  * ==Per-case semantics==
  *
  * - [Inserted]: a brand-new entity was created. `ref` is the
  *   new publication identity (typically version=1).
  * - [Updated]: an existing entity was updated. `previous` is
  *   the old [CatalogRef] (just before the update); `current`
  *   is the new [CatalogRef] (just after the update). The
  *   publisher can verify the version bump + new digest.
  * - [Conflict]: the publication was rejected because the
  *   `PublishMode` precondition was not met. `current` is
  *   `Some(ref)` if the entity exists at the requested
  *   identity (the publisher can retry from the new state);
  *   `None` if the conflict was a different kind (e.g.
  *   `PublishMode.CreateOnly` on an existing entity still
  *   produces [Conflict], but `current` is `Some(ref)`).
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data (sealed trait + final case classes)
  * - Equality auto-derived
  * - `Product with Serializable`
  */
sealed trait PublishResult extends Product with Serializable

object PublishResult {

  /** The entity was created. `ref` is the new publication
    * identity (typically version=1). */
  final case class Inserted(ref: CatalogRef) extends PublishResult

  /** The entity was updated. `previous` is the ref just
    * before the update; `current` is the ref just after. */
  final case class Updated(
      previous: CatalogRef,
      current:  CatalogRef,
  ) extends PublishResult

  /** The publication was rejected. `current` is the entity's
    * current ref (if it exists); `reason` describes why. */
  final case class Conflict(
      current: Option[CatalogRef],
      reason:  String,
  ) extends PublishResult
}