package io.semanticdf.core.model

import io.semanticdf.core.model.ProviderRef

/** Engine-portable rollup-registration ADT — Phase 2 contract.
  * Mirrors the design doc §4.3.3 "RollupRegistration".
  *
  * A [[RollupRegistration]] ties a [[RollupSpec]] (the portable
  * metadata) to a [[ProviderRef]] (the engine-side data source)
  * plus a [[RollupPrecompute]] (the engine-side computed stats).
  *
  * ==Why this exists (vs. fields on `RollupSpec`)==
  *
  * `RollupSpec` carries no DataFrame reference (per the design:
  * "ManifestDocument stores RollupSpec only"). The provider
  * reference is registered separately, at the engine adapter
  * layer, in a `RollupRegistration`. This is the canonical pattern
  * for the v0.3.0 portable model:
  *   - `RollupSpec` flows through the portable model + the v2 manifest
  *   - `RollupRegistration` ties the spec to an engine-specific provider
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior lives
  * elsewhere"): the registration is the BOUNDARY between the
  * portable spec and the engine-specific provider. The provider
  * reference (a `() => DataFrame` thunk, for Spark) is engine-side;
  * the spec is portable.
  *
  * ==Why `provider: ProviderRef` (not a `() => DataFrame` closure)==
  *
  * `ProviderRef` is a portable REFERENCE (a name like
  * `"dataFrame:ordersByRegion"`). The actual `() => DataFrame`
  * closure lives in the engine-specific `SparkProviderRegistry`
  * (per the design's "Registry ownership and provider lookup"
  * section). The portable model only references the name; the
  * engine adapter resolves the name to the closure.
  *
  * ==Why `precomputed: RollupPrecompute` is computed at registration time==
  *
  * Per the design: "Portable rollups carry no precomputed fields.
  * Registration computes them through the provider on the engine
  * side". The engine adapter calls the provider thunk ONCE at
  * registration to compute `rowCount` + `columns` + `sourceDigest`;
  * the closure result is discarded (the closure is the entry point
  * to the registry, not the data itself).
  *
  * ==Why core (engine-portable)==
  *
  * The registration SHAPE (spec + provider ref + precompute) is
  * universal across engines. The provider RESOLUTION (calling the
  * provider thunk, computing the precompute) is engine-specific —
  * that's in the engine adapter.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: case class (no behavior)
  * - Equality auto-derived
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/RollupRegistration.scala`
  */
final case class RollupRegistration(
    spec:       RollupSpec,
    provider:   ProviderRef,
    precomputed: RollupPrecompute,
) extends Product with Serializable