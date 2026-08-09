package io.semanticdf.core.manifest

import io.semanticdf.core.model.ExtensionValue

/** Engine-portable manifest shape — v1 of the YAML loader output.
  *
  * Per the v0.3.2 design doc (PR #437): this is the YAML-reader
  * intermediate. The `YamlManifestLoader` (3A.2) reads a YAML file
  * into a `PortableModel`, then converts it to `core.Model` via
  * `Model.of`.
  *
  * ==Why a separate shape (vs. reading directly into `core.Model`)==
  *
  * Two reasons:
  *   1. **Separation of concerns**: parse logic stays separate from
  *      domain validation. The portable types hold raw user-supplied
  *      data; the domain types (`core.Model`, `core.Dimension`, etc.)
  *      hold validated data. Mixing the two means parse failures
  *      could leak into domain invariants.
  *   2. **Format flexibility**: portable types are 1-to-1 with the
  *      YAML schema. If the YAML format evolves, only this layer
  *      changes; the domain layer stays stable.
  *
  * ==Why `String` for expressions (not `core.expr.Expr`)==
  *
  * Per the v0.3.2 design doc §6.3: portable YAML holds raw SQL
  * strings. The reader (3A.2) converts them to `Expr.SqlString`
  * via the existing SQL parser (or wraps them as-is for engines
  * that accept raw SQL). Future work (post-v0.3.2) could add a
  * structured `Expr` representation.
  *
  * ==Why no `require`/validation in this layer==
  *
  * Per scala-data-driven-refacer §2 ("shape and validity are
  * separate"): the `PortableModel` is unconditional. Validity
  * is enforced by `Model.of` (existing smart constructor) after
  * the reader converts portable → domain.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `final case class` (no behavior)
  * - `extends Product with Serializable` for Jackson round-trip +
  *   Spark serialization (the latter matters for `core.Model`
  *   downstream consumers)
  * - All collections are immutable (`List`, `Map`)
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/manifest/` */
final case class PortableModel(
    name:               String,
    description:        Option[String]               = None,
    source:             PortableSource,
    dimensions:         List[PortableDimension]       = Nil,
    measures:           List[PortableMeasure]         = Nil,
    calculatedMeasures: List[PortableCalculatedMeasure] = Nil,
    joins:              List[PortableJoin]            = Nil,
    filters:            List[PortableFilter]          = Nil,
    rollups:            List[PortableRollup]          = Nil,
    version:            Int                           = 1,
    status:             PortableStatus                = PortableStatus.Draft,
    extensions:         Map[String, ExtensionValue]   = Map.empty,
) extends Product with Serializable
