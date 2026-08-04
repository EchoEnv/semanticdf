package io.semanticdf.core.model

/** Engine-portable mirror of `io.semanticdf.ModelStatus` —
  * Phase 1 increment 10: data-only model lifecycle status.
  *
  * Mirrors the original `io.semanticdf.ModelStatus` (sealed trait +
  * 3 case objects + pure helper functions) verbatim. The original
  * file is already Spark-free (no `org.apache.spark.*` imports)
  * and behavior-light (only pure functions: `asString`, `fromString`,
  * `all`).
  *
  * ==Why this exists==
  *
  * Future engine adapters (Trino, Databricks, custom-platform) need
  * to surface model lifecycle status (Published / Draft / Deprecated)
  * when their queries run. They can do this without depending on
  * Spark by using this core mirror.
  *
  * The original `io.semanticdf.ModelStatus` remains the canonical
  * Spark-facing type (it may grow engine-coupled methods in the
  * future). Engine-portable consumers (e.g. wire-format encoders,
  * the MCP `describe_model` field, OKF generation) should depend on
  * this core version instead.
  *
  * ==Boundary contract==
  *
  * This file compiles with zero `org.apache.spark.*` imports.
  * Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/ModelStatus.scala`
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 3 case objects
  * - Pure functions: `asString`, `fromString`, `all` are referentially
  *   transparent (same input → same output, no side effects)
  * - Equality auto-derived (case objects are singletons)
  * - Wire-stable string contract preserved: `asString` returns
  *   `"draft"` / `"published"` / `"deprecated"` — these are part of
  *   the MCP wire format and must not change.
  */
sealed trait ModelStatus extends Product with Serializable {
  /** Wire-stable lowercase string. */
  def asString: String = this match {
    case ModelStatus.Draft       => "draft"
    case ModelStatus.Published   => "published"
    case ModelStatus.Deprecated  => "deprecated"
  }
}

object ModelStatus {

  /** Authoring state. The model may be incomplete, may change shape without
    * notice, and may not yet produce correct results. Use for in-progress
    * work shared among collaborators before public exposure. */
  case object Draft extends ModelStatus

  /** Default state. The model is intended for query and is the model's
    * declared authoritative definition. Carries no SLA guarantees; the
    * consumer (MCP / agent / dashboard) decides how to interpret it. */
  case object Published extends ModelStatus

  /** End-of-life. The model still compiles and queries succeed, but the
    * author has committed to removing or replacing it. Consumers SHOULD
    * warn before serving results; future MCP work will refuse by default
    * for deprecated models (out of scope here). */
  case object Deprecated extends ModelStatus

  /** Parse the wire format. Case-insensitive on input. Returns `None` for
    * unknown values — callers should reject unknown status in strict
    * contexts (e.g. `YamlLoader`) and accept-anything in tolerant ones. */
  def fromString(s: String): Option[ModelStatus] = s.toLowerCase match {
    case "draft"      => Some(Draft)
    case "published"  => Some(Published)
    case "deprecated" => Some(Deprecated)
    case _            => None
  }

  /** All known statuses, in display order. Useful for tests and for
    * enumerating accepted YAML values in error messages. */
  def all: Seq[ModelStatus] = Seq(Draft, Published, Deprecated)
}