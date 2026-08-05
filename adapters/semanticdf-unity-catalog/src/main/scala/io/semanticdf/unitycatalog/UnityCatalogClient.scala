package io.semanticdf.unitycatalog

import io.semanticdf.core.schema.{Field, SealedDataType}

/** Engine-specific Unity Catalog client boundary trait.
  *
  * This trait is the boundary between the engine-portable
  * `SourceResolver` contract (in core) and the engine-specific
  * Unity Catalog REST API. The `UnityCatalogSourceResolver`
  * depends on this trait (not on a concrete HTTP client), so
  * tests can inject a fake implementation.
  *
  * ==Why a trait (vs. a concrete HTTP client)==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): the BEHAVIOR (calling UC, parsing JSON)
  * is engine-specific. The CONTRACT (the methods the resolver
  * needs) is in this trait — it's a small abstraction that's
  * justified by testability needs (the same justification as
  * `TrinoClient`).
  *
  * ==Why core has no `UnityCatalogClient`==
  *
  * `UnityCatalogClient` is engine-specific (only the UC
  * adapter uses it). It lives in the Trino adapter, NOT in
  * core. The core contract is `SourceResolver` (engine-
  * portable); each catalog adapter provides its own resolver
  * AND its own client boundary.
  *
  * ==Why each method returns either `Option` or a typed result==
  *
  * `describeTable` returns `Option[UcTableSchema]` because the
  * table might not exist. Per the multi-engine design §4.3.2,
  * `Option.None` from this method maps to `ResolvedSource.NotFound`
  * in the resolver.
  *
  * ==Why this is a `catalog/SourceResolver` pattern (not engine-specific)==
  *
  * Per the multi-engine design §4.6 layer-separation principle:
  * the catalog contract is independent of the engine. The
  * `UnityCatalogClient` produces engine-portable schema data;
  * ANY engine (Trino, Spark, DuckDB, ...) can consume it via
  * its own `SourceResolver` + `UnityCatalogClient` composition.
  * This file lives in `semanticdf-trino` for now because that's
  * where Trino consumes it; future PRs may extract it to a
  * dedicated `semanticdf-catalog-unity` module.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/src/main/scala/io/semanticdf/trino/UnityCatalogClient.scala`
  */
trait UnityCatalogClient extends Serializable {

  /** Describe a table: return its columns (name + type) in the
    * order Unity Catalog reports them.
    *
    * Returns `None` if the catalog/schema/table doesn't exist or
    * the user doesn't have permission to describe it (auth
    * failure is mapped to `ResolvedSource.AuthFailed` by the
    * caller).
    *
    * @param catalog the catalog name (e.g. "unity")
    * @param schema  the schema / namespace name (e.g. "public")
    * @param table   the table name */
  def describeTable(
      catalog: String,
      schema:  String,
      table:   String,
  ): Option[UcTableSchema]
}

/** Engine-portable description of a table returned by
  * [[UnityCatalogClient.describeTable]]. Just the data the
  * resolver needs (column name + type); no engine-specific
  * metadata. */
final case class UcTableSchema(
    catalog: String,
    schema:  String,
    table:   String,
    columns: List[UcColumn],
) extends Product with Serializable

/** Engine-portable description of a single column. The
  * `dataType` is the UC type-name string (e.g. `"LONG"`,
  * `"STRING"`, `"DECIMAL"`); the resolver maps it to a portable
  * [[SealedDataType]] via [[ucTypeToSealedDataType]]. */
final case class UcColumn(
    name:     String,
    dataType: String,
    nullable: Boolean,
) extends Product with Serializable