package io.semanticdf.hivemetastore

/** Engine-specific Hive Metastore client boundary trait.
  *
  * Mirrors `UnityCatalogClient` (in `adapters/semanticdf-unity-catalog`)
  * — the `HiveMetastoreSourceResolver` depends on this trait
  * (not on a concrete HMS Thrift client), so tests can inject
  * a fake implementation.
  *
  * ==Why a trait (vs. a concrete HMS Thrift client)==
  *
  * Per scala-data-driven-refacer §1: the BEHAVIOR (calling HMS,
  * parsing Thrift responses) is engine-specific. The CONTRACT
  * (the methods the resolver needs) is in this trait — small
  * abstraction justified by testability.
  *
  * ==Why the SHAPE mirrors `UnityCatalogClient`==
  *
  * Both adapters serve the same `SourceResolver` contract. The
  * shape (`describeTable(catalog, schema, table) -> Option[HmsTableSchema]`)
  * is identical — only the implementation differs (Thrift vs.
  * REST). Tests for `HiveMetastoreSourceResolverSpec` mirror
  * `UnityCatalogSourceResolverSpec` almost verbatim.
  *
  * ==Why core has no `HiveMetastoreClient`==
  *
  * Engine-specific (only HMS uses it). Lives in the adapter,
  * not in core.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
trait HiveMetastoreClient extends Serializable {

  /** Describe a table: return its columns (name + type) in the
    * order HMS reports them.
    *
    * Returns `None` if the catalog/database/table doesn't exist
    * (per the design §4.3.2, mapped to `ResolvedSource.NotFound`
    * by the caller).
    *
    * @param catalog the catalog name (HMS calls this "database"
    *                in older versions; HMS 3.x uses "catalog")
    * @param database the database name (HMS "schema")
    * @param table    the table name */
  def describeTable(
      catalog: String,
      database: String,
      table:   String,
  ): Option[HmsTableSchema]
}

/** Engine-portable description of a table returned by
  * [[HiveMetastoreClient.describeTable]]. Just the data the
  * resolver needs (column name + type); no HMS-specific metadata. */
final case class HmsTableSchema(
    catalog: String,
    database: String,
    table:   String,
    columns: List[HmsColumn],
) extends Product with Serializable

/** Engine-portable description of a single column. The `dataType`
  * is the HMS type-name string (e.g. `"bigint"`, `"string"`,
  * `"decimal(18,2)"`); the resolver maps it to a portable
  * [[io.semanticdf.core.schema.SealedDataType]] via
  * [[hmsTypeToString]] (passthrough — same approach as
  * `UnityCatalogClient`). */
final case class HmsColumn(
    name:     String,
    dataType: String,
    nullable: Boolean,
) extends Product with Serializable