package io.semanticdf.core.catalog

/** Engine-portable catalog-entity kind ADT \u2014 added in v0.3.0.
  *
  * Closed ADT: `Model`, `Rollup`, `ExtensionBlob`.
  *
  * Identifies WHAT kind of thing is being published to / discovered
  * from a catalog. The closed ADT forces every adapter to declare
  * which entity kinds it supports (no stringly-typed magic).
  *
  * ==Per-case semantics==
  *
  * - [Model]: a semantic model (the primary entity). Adapters MUST
  *   support this case.
  * - [Rollup]: a rollup definition. Adapters MAY support this case.
  * - [ExtensionBlob]: an external extension blob (large model
  *   metadata that doesn't fit inline). Adapters MAY support this
  *   case.
  *
  * ==Why a sealed ADT (not a String)==
  *
  * Per scala-data-driven-refacer \u00a73: a rule (entity kinds)
  * becomes data only when it must change without a deploy. The set
  * is FIXED at compile time (3 cases); using a String would let
  * callers invent new kinds that the adapter can't classify.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports.
  */
sealed trait CatalogEntity extends Product with Serializable

object CatalogEntity {

  /** A semantic model. Adapters MUST support this case. */
  case object Model extends CatalogEntity

  /** A rollup definition. Adapters MAY support this case. */
  case object Rollup extends CatalogEntity

  /** An external extension blob (large model metadata). Adapters
    * MAY support this case. */
  case object ExtensionBlob extends CatalogEntity
}