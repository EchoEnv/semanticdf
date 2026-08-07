package io.semanticdf.core.engine

import io.semanticdf.core.rel.JoinKind

/** Structured value-object view of an engine's advertised
  * capabilities \u2014 v0.3.0.
  *
  * Replaces the loose `Map[Capability, String]` that used to be
  * returned by per-engine `describeCapabilities`. The structured
  * shape gives consumers (MCP `describe_model`, OKF generation,
  * future UI) typed fields to route on, instead of forcing them
  * to parse strings.
  *
  * ==Why a value object (vs. just `Map[Capability, String]`)==
  *
  * The original `Map[Capability, String]` was typed (the keys
  * are typed `Capability`), but every consumer that wanted to
  * do anything useful had to parse the description string (e.g.
  * "supports window functions" \u2192 boolean). The structured
  * fields (`supportedJoinKinds`, `supportsRollup`,
  * `supportsMaterialize`) eliminate the parse step.
  *
  * The `descriptions` map is preserved (for human-readable
  * MCP output) but is no longer the only source of information.
  *
  * ==Per-field contract==
  *
  * - `identity`: wire-stable engine label (mirrors `Engine.identity`)
  * - `descriptions`: typed per-capability descriptions, used by
  *   MCP `describe_model`
  * - `supportedJoinKinds`: which join kinds the engine supports
  *   (subset of the 5-case `JoinKind` ADT). Empty means "unknown
  *   / not advertised"; a non-empty set is a positive claim.
  * - `supportsRollup`: whether the engine supports rollup precompute
  *   (cached aggregated results)
  * - `supportsMaterialize`: whether the engine supports materializing
  *   intermediate results (e.g. `df.persist()` style)
  *
  * ==Closed invariant (enforced by per-engine tests)==
  *
  * Every capability in `descriptions` MUST also be in the
  * engine's `capabilities: Set[Capability]` set. This invariant
  * is enforced by per-engine tests
  * ("describeCapabilities has an entry for every capability in
  * `capabilities`"). Promoting it to a construction-time
  * `require` would couple the type to the engine's own
  * `capabilities` set, which is too much coupling for a
  * portable value object.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/EngineCapabilities.scala`
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: final case class (no behavior)
  * - Equality + hash codes auto-derived (case class)
  * - `Product with Serializable` (auto-derived)
  */
final case class EngineCapabilities(
    identity:           String,
    descriptions:       Map[Capability, String],
    supportedJoinKinds: Set[JoinKind]    = Set.empty,
    supportsRollup:     Boolean          = false,
    supportsMaterialize: Boolean         = false,
) extends Product with Serializable {

  /** Set of capabilities that have descriptions. */
  def described: Set[Capability] = descriptions.keySet

  /** Convenience: look up a description by capability. */
  def describe(c: Capability): Option[String] = descriptions.get(c)
}