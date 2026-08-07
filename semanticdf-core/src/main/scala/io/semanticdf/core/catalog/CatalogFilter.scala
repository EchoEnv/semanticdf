package io.semanticdf.core.catalog

/** Engine-portable catalog-filter record \u2014 PR 10 of the v0.3.0
  * deferred-work triage.
  *
  * Used by [CatalogAdapter.list] to filter the listing. All fields
  * are Optional so the caller can scope the listing by any
  * combination of (catalog, namespace, name prefix).
  *
  * ==Per-field semantics==
  *
  * - `catalog`:   exact match on catalog name (None = no filter)
  * - `namespace`: exact match on namespace (None = no filter)
  * - `namePrefix`: prefix match on entity name (None = no filter);
  *   useful for listing all entities starting with "orders_".
  * - `kind`:      filter by [CatalogEntity] kind (None = no filter)
  * - `limit`:     maximum number of entries to return (None = no limit)
  *
  * ==Why a record (not a String map)==
  *
  * Per scala-data-driven-refacer \u00a71: pure data with typed
  * fields. A String map would let callers invent new filter keys
  * that the adapter can't classify.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports.
  */
final case class CatalogFilter(
    catalog:    Option[String]         = None,
    namespace:  Option[String]         = None,
    namePrefix: Option[String]         = None,
    kind:       Option[CatalogEntity]  = None,
    limit:      Option[Int]            = None,
) extends Product with Serializable {

  /** True when this filter would match every entity (no filters
    * applied). Useful for tests and the "list everything"
    * code path. */
  def isEmpty: Boolean =
    catalog.isEmpty && namespace.isEmpty && namePrefix.isEmpty && kind.isEmpty && limit.isEmpty
}