package io.semanticdf.core.schema

import io.semanticdf.core.model.SourceRef

/** Engine-portable relational scan —
  * Phase 2 contract. Mirrors the design doc §12 "ResolvedScan".
  *
  * A `ResolvedScan` is the result of a successful source resolution
  * (the "scan" case of the `ResolvedSource` ADT — see
  * `core.engine.ResolvedSource.Scan`). It carries the original
  * `SourceRef` (for provenance), the resolved fields, and the
  * projection (the subset of fields the model wants).
  *
  * ==Why a separate type from `SourceRef`==
  *
  * `SourceRef` is the IDENTITY (what the model says it wants).
  * `ResolvedScan` is the RESOLUTION (what the engine actually
  * produced). They live in different packages (`core.model` vs
  * `core.schema`) because they belong to different concerns.
  *
  * ==Why `fields` and `projection` are separate==
  *
  * `fields` is the FULL set of fields the source resolved to. The
  * engine's source resolver (e.g. Trino's `DESCRIBE orders`) returns
  * ALL columns. `projection` is the subset the model actually
  * wants — the model's `SELECT field1, field2 FROM ...` clause.
  *
  * The plan optimizer can push the projection down to the source
  * (column pruning) — that's an engine-specific optimization.
  * The portable model declares the projection; the engine
  * optimizer decides whether to actually push it down.
  *
  * ==Why core (engine-portable)==
  *
  * The resolved scan is the contract that flows through the
  * portable model after source resolution. Per scala-data-driven-
  * refactor, the data (the resolved scan) lives in core; the
  * behavior (the engine-specific source resolver) lives in the
  * engine adapter layer.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` (no behavior)
  * - Equality auto-derived (case class)
  * - `Product with Serializable` for Java-serialization round-trip
  * - Field names are wire-stable (per Field.scala)
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/schema/ResolvedScan.scala`
  */
final case class ResolvedScan(
    source:    SourceRef,
    fields:    Seq[Field],
    projection: Seq[String] = Seq.empty,
) extends Product with Serializable

object ResolvedScan {

  /** A `ResolvedScan` with no projection (i.e. all fields are
    * selected). This is the common case for `SELECT * FROM ...`
    * — the engine reads all columns. */
  def full(source: SourceRef, fields: Seq[Field]): ResolvedScan =
    ResolvedScan(source, fields, projection = Seq.empty)
}