package io.semanticdf.core.catalog

/** Engine-portable typed catalog ref \u2014 added in v0.3.0.
  *
  * Extracted the stable `identity: CatalogIdentity` from
  * `(catalog, namespace, name)`. Closes design finding #13
  * ("Version catalog identity and define create-only/upsert/CAS
  * results", \u00a75.3). Pre-v0.3.0 the design returned an undefined,
  * unversioned `CatalogRef`.
  *
  * ==Why five fields==
  *
  * - `catalog`:    the catalog name (e.g. "unity", "hms_prod")
  * - `namespace`:  the schema / database name (e.g. "public")
  * - `name`:       the entity name (e.g. "orders", "flights")
  * - `version`:    monotonic version (every publish increments)
  * - `digest`:     hash of the entity content (for CAS)
  *
  * The (catalog, namespace, name) tuple is the stable identity; the
  * (version, digest) pair is the publication identity. Two refs with
  * the same (catalog, namespace, name) but different versions are
  * different publication states of the same logical entity.
  *
  * ==Why a `final case class`==
  *
  * Per scala-data-driven-refacer \u00a71: pure data, equality auto-
  * derived, `Product with Serializable`. The digest is a String for
  * wire-format stability (not a `Long` or a `Digest` ADT \u2014 the
  * digest algorithm is the publisher's choice; we don't lock it in).
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data (final case class, no behavior)
  * - Equality + hash codes auto-derived
  * - `Product with Serializable`
  */
final case class CatalogRef(
    catalog:   String,
    namespace: String,
    name:      String,
    version:   Int,
    digest:    String,
) extends Product with Serializable {

  /** The stable identity of the entity (ignoring version/digest).
    * Two refs with the same identity refer to the same logical
    * entity, possibly at different publication versions. */
  def identity: CatalogIdentity =
    CatalogIdentity(catalog = catalog, namespace = namespace, name = name)
}

/** The stable (catalog, namespace, name) identity of an entity,
  * independent of its current publication version. */
final case class CatalogIdentity(
    catalog:   String,
    namespace: String,
    name:      String,
) extends Product with Serializable