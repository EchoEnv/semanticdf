package io.semanticdf.core.engine

/** Engine-portable sealed ADT for non-fatal plan diagnostics —
  * Phase 2 contract. Mirrors the design doc §4 "Engine contract".
  *
  * Engine adapters return \`EngineWarning\` alongside successful results
  * to surface non-fatal diagnostics (policy adaptations, implicit
  * assumptions, capability hesitation). Warnings are TYPED — never
  * arbitrary strings. The MCP envelope's \`warnings\` array carries
  * these typed values; consumers can route on the case class.
  *
  * ==Why this exists==
  *
  * Without a typed warning ADT, engines would log free-form strings
  * that consumers couldn't programmatically route on. \`SparkPolicyAdapted\`
  * vs \`ImplicitAssumptionMade\` have different remediation paths;
  * the consumer (MCP / agent / dashboard) needs to distinguish them.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + final case classes
  * - Each case carries exactly the fields needed to identify the warning
  * - Equality auto-derived
  * - \`Product with Serializable\` for Java-serialization round-trip
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * \`grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/engine/EngineWarning.scala\`
  */
sealed trait EngineWarning extends Product with Serializable

object EngineWarning {

  /** The engine adapted the requested policy to its supported form.
    * E.g. a \`persist(MEMORY_ONLY)\` request was honored via the
    * engine's closest equivalent (in-memory caching on Trino, etc.).
    * \`original\` is the requested policy; \`adapted\` is what the
    * engine actually used. Consumers SHOULD surface this in their
    * envelope so the agent can decide whether to retry with a
    * different policy. */
  final case class PolicyAdapted(
      original: String,
      adapted:  String,
  ) extends EngineWarning

  /** The engine made an implicit assumption (e.g. "treating this
    * column as nullable because the source didn't specify nullability").
    * \`name\` identifies which assumption. Consumers SHOULD warn so
    * users can verify the assumption is correct for their data. */
  final case class ImplicitAssumptionMade(
      name: String,
  ) extends EngineWarning

  /** The engine was asked for a capability it technically supports
    * but with reluctance (e.g. it works but is known to be slow for
    * the given data shape). Consumers SHOULD warn. */
  final case class CapabilityReluctantlySupported(
      name: String,
  ) extends EngineWarning
}