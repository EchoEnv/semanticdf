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

  /** Create a table in HMS with the given columns and parameters.
    *
    * v0.3.1 (Gap 7 closure): publish-side method for
    * [[HiveMetastoreCatalogAdapter]]. Parameters are stored in
    * HMS's `Table.parameters` map (the only mutable metadata
    * HMS 3.x exposes); the [[HiveMetastoreCatalogAdapter]] uses
    * three reserved keys for CAS:
    *
    *   - `semanticdf_kind`    : "model" | "rollup" | "extension_blob"
    *   - `semanticdf_version` : the entity version (Int, stringified)
    *   - `semanticdf_digest`  : the entity digest
    *
    * @return `Right(())` on success, `Left(CatalogError.Unauthorized)`
    *         on permission failure, `Left(CatalogError.Network)` on
    *         transport failure, `Left(CatalogError.MalformedManifest)`
    *         on schema validation failure */
  def createTable(
      catalog:    String,
      database:   String,
      table:      String,
      columns:    List[HmsColumn],
      parameters: Map[String, String],
  ): Either[io.semanticdf.core.catalog.CatalogError, Unit]

  /** Update a table's parameters in HMS (used for atomic CAS —
    * overwriting the `semanticdf_*` keys is the commit step).
    *
    * @return `Right(())` on success, `Left(CatalogError.Conflict)`
    *         if the table doesn't exist, `Left(CatalogError.Network)`
    *         on transport failure */
  def updateTableParameters(
      catalog:    String,
      database:   String,
      table:      String,
      parameters: Map[String, String],
  ): Either[io.semanticdf.core.catalog.CatalogError, Unit]

  /** Get a table's current parameters. Returns `None` if the table
    * doesn't exist.
    *
    * Used by [[HiveMetastoreCatalogAdapter]] to read the current
    * `semanticdf_digest` for CAS verification. */
  def getTableParameters(
      catalog:  String,
      database: String,
      table:    String,
  ): Either[io.semanticdf.core.catalog.CatalogError, Option[Map[String, String]]]

  /** List tables in a database, optionally filtered by a name prefix.
    * Returns `Right(Nil)` if the database is empty or doesn't exist
    * (matches the trait's "may not exist" semantics).
    *
    * Used by [[HiveMetastoreCatalogAdapter]] to implement the
    * `list(filter)` method. */
  def listTables(
      catalog:  String,
      database: String,
      prefix:   String,
  ): Either[io.semanticdf.core.catalog.CatalogError, List[String]]
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