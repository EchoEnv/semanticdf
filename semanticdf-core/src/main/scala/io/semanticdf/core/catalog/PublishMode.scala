package io.semanticdf.core.catalog

/** Engine-portable publication-mode ADT \u2014 PR 10 of the v0.3.0
  * deferred-work triage.
  *
  * Defines how a [CatalogAdapter.publish] call interacts with an
  * existing publication of the same identity. The closed ADT
  * forces the adapter to handle the closed set of modes (no
  * stringly-typed magic).
  *
  * ==Per-case semantics==
  *
  * - [CreateOnly]: create the entity; if it already exists at
  *   the same identity, return [PublishResult.Conflict]. The
  *   publisher is asserting "this is a brand-new entity".
  * - [Upsert]: create if absent, update if present (atomically
  *   increments the version). The publisher is asserting "I
  *   want the latest version of this entity to reflect my
  *   content, regardless of who else is publishing".
  * - [CompareAndSet]: update ONLY if the current digest matches
  *   `expectedDigest`. The publisher is asserting "I'm updating
  *   from a known state; if anyone else has updated since,
  *   refuse the update (return [PublishResult.Conflict])". This
  *   is the canonical optimistic-concurrency pattern.
  *
  * ==Why a sealed ADT (not a String)==
  *
  * A String parameter would let callers pass `"create-only"`,
  * `"upsert"`, typos, or arbitrary verbs. The closed ADT forces
  * the publisher to declare its intent in code (compile-time
  * check on add).
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data (sealed trait + final case classes / case objects)
  * - Equality auto-derived
  * - `Product with Serializable`
  */
sealed trait PublishMode extends Product with Serializable

object PublishMode {

  /** Create the entity. Fails if it already exists at the
    * same identity. */
  case object CreateOnly extends PublishMode

  /** Create if absent, update if present (atomic version
    * increment). */
  case object Upsert extends PublishMode

  /** Compare-and-set: update only if the current digest
    * matches `expectedDigest`. Returns [PublishResult.Conflict]
    * otherwise. */
  final case class CompareAndSet(expectedDigest: String) extends PublishMode
}