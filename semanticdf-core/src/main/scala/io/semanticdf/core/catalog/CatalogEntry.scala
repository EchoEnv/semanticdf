package io.semanticdf.core.catalog

/** Engine-portable catalog-entry record \u2014 PR 10 of the v0.3.0
  * deferred-work triage.
  *
  * Returned by [CatalogAdapter.list] for each entry that matches
  * the filter. Carries the typed [CatalogRef] + a short summary
  * (the entity kind + a small metadata map).
  *
  * ==Why a `Map[String, String]` summary==
  *
  * The summary is the place for catalog-specific metadata (e.g.
  * "last_modified_at" -> "2024-01-15T10:30:00Z",
  * "owner" -> "analytics"). The keys are catalog-specific
  * (the adapter decides); we don't lock them in.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data (final case class, no behavior)
  * - Equality + hash codes auto-derived
  * - `Product with Serializable`
  */
final case class CatalogEntry(
    ref:     CatalogRef,
    kind:    CatalogEntity,
    summary: Map[String, String] = Map.empty,
) extends Product with Serializable