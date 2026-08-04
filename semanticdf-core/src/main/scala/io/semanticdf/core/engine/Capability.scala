package io.semanticdf.core.engine

/** Engine-portable typed capability name —
  * Phase 2 contract. Mirrors the design doc §4 "Engine contract".
  *
  * A `Capability` is a TYPED name for one engine feature (e.g.
  * "nested-struct-types", "broadcast-join", "window-function-rank").
  * It's used as the `name` field of `EngineError.UnsupportedCapability`
  * and as a parameter to the future `Engine.supports` method.
  *
  * ==Why a sealed trait and not a string==
  *
  * Per the design doc: "engine adapters return `EngineError`... the
  * `UnsupportedCapability` case carries a `Capability` typed name."
  * The closed ADT forces the engine adapter to declare its unsupported
  * capabilities in code (compile-time check on add), not as a free-form
  * string. Free-form strings would let engine adapters accidentally
  * invent new capability names that the MCP server can't classify.
  *
  * ==Why two shapes (case objects + factory)==
  *
  * - The case objects (`NestedStructTypes`, `BroadcastJoin`, etc.)
  *   are the closed enumeration — the canonical, known capabilities.
  *   Pattern-matching on them is exhaustive at compile time.
  * - The `Named(name)` factory creates a user-defined capability for
  *   engine adapters that need a custom one. Pattern-matching on
  *   user-defined ones uses the string name.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + final case classes (no behavior)
  * - Equality auto-derived (case classes)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/Capability.scala`
  */
sealed trait Capability extends Product with Serializable {
  /** Wire-stable name. */
  def name: String
}

object Capability {

  /** Factory for engine-defined (non-enumeration) capabilities.
    * Pattern-matching on the result uses the `name` field. */
  final case class Named(name: String) extends Capability

  // -- Common capabilities (closed enumeration; add more as needed) ---------

  /** Nested struct / row types in source data. Not all engines support
    * (e.g. SQLite has limited struct support). */
  case object NestedStructTypes extends Capability { val name = "nested-struct-types" }

  /** Broadcast join hint — push a small dimension to all executors. */
  case object BroadcastJoin extends Capability { val name = "broadcast-join" }

  /** Skew handling — salt the join key on a hot dimension. */
  case object SkewJoin extends Capability { val name = "skew-join" }

  /** Window functions with `RANK()` / `ROW_NUMBER()` style. */
  case object WindowRanking extends Capability { val name = "window-ranking" }

  /** Materialize / persist intermediate results (e.g. MEMORY_ONLY). */
  case object Materialize extends Capability { val name = "materialize" }

  /** Late-binding providers — `() => DataFrame` style at runtime. */
  case object LateBinding extends Capability { val name = "late-binding" }
}