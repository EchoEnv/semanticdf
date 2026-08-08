package io.semanticdf.myplatform

import io.semanticdf.core.engine.{ResolvedSchema, ResolvedSource}
import io.semanticdf.core.model.SourceRef

/** Boundary trait for MyPlatform data-plane operations.
  *
  * Mirrors `io.semanticdf.hera.HeraClient` and
  * `io.semanticdf.unitycatalog.UnityCatalogClient`.
  *
  * ==Why a trait (vs. a concrete HTTP / JDBC client)==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): the SHAPE of the contract is here; the BODY (the
  * actual HTTP / JDBC / Thrift calls) is in the concrete impl.
  *
  * Testability — `FakeMyPlatformClient` (in `src/test`) can return
  * scripted responses without needing a real MyPlatform instance.
  *
  * ==Why all data-plane methods return `Either[MyPlatformError, X]`==
  *
  * Per `docs/design/error-handling-style.md`: public APIs must return
  * `Either[L, X]` where `L` is a sealed ADT (not a string or a
  * `Throwable`). [[MyPlatformError]] is the ADT; it has 8 SPECIFIC
  * failure cases (no catch-all `ServerError`) so callers can
  * distinguish network from auth from query-syntax from CAS-conflict.
  *
  * ==Why no `getTableProperties` (vs UC's pattern)==
  *
  * UC's adapter reads the `properties` map for CAS. If your platform
  * uses a different CAS mechanism (version field, dedicated digest,
  * etc.), define it here. The CAS surface lives in
  * [[MyPlatformCatalogAdapter]], not in this trait.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
trait MyPlatformClient extends Serializable {

  /** Execute a SQL query against MyPlatform (sync mode).
    *
    * Per error-handling-style.md: returns `Either[MyPlatformError, X]`.
    *
    * @param sql       the SQL string to execute
    * @param realmId   the realm scope for the query
    * @param limit     max rows to return (server-side enforced) */
  def executeQuery(
      sql:     String,
      realmId: String,
      limit:   Int = 100,
  ): Either[MyPlatformError, MyPlatformResult]

  /** Describe a table's schema.
    *
    * Returns `Left(MyPlatformError.NotFound)` if the table doesn't
    * exist. */
  def describeTable(
      table:   String,
      realmId: String,
  ): Either[MyPlatformError, ResolvedSchema]

  /** Get a table's metadata (used by the catalog adapter for CAS).
    *
    * Returns `Left(MyPlatformError.NotFound)` if the table doesn't
    * exist. */
  def getTableMeta(
      table:   String,
      realmId: String,
  ): Either[MyPlatformError, MyPlatformTableMeta]

  /** Create a new table (CreateOnly mode).
    *
    * Returns `Left(MyPlatformError.AlreadyExists)` if a table with
    * the same name exists. */
  def createTable(
      table:   String,
      realmId: String,
      meta:    MyPlatformTableMeta,
  ): Either[MyPlatformError, MyPlatformTableMeta]

  /** Update a table's metadata with CAS via the version field.
    *
    * Returns `Left(MyPlatformError.Conflict)` if the current
    * version doesn't match `expectedVersion`. */
  def updateTable(
      table:           String,
      realmId:         String,
      meta:            MyPlatformTableMeta,
      expectedVersion: Long,
  ): Either[MyPlatformError, MyPlatformTableMeta]

  /** List tables in a realm, optionally filtered by name prefix.
    *
    * Returns `Right(Nil)` if the realm has no tables or doesn't exist. */
  def listTables(
      realmId: String,
      prefix:  String,
  ): Either[MyPlatformError, List[String]]

  /** Resolve a source ref's realm (used by the source resolver to
    * map a portable `SourceRef.ByName.catalog` to a realm). */
  def resolveRealmId(catalogName: String): Option[String]
}

/** Engine-portable description of a MyPlatform query result.
  *
  * Per scala-data-driven-refacer §1: pure data, `Product with Serializable`. */
final case class MyPlatformResult(
    fields: List[MyPlatformField],
    rows:   List[Map[String, Any]],
    queryTime: java.time.Duration = java.time.Duration.ZERO,
) extends Product with Serializable

/** Engine-portable description of a MyPlatform table column. */
final case class MyPlatformField(
    name:     String,
    dataType: String,
    nullable: Boolean,
) extends Product with Serializable

/** Engine-portable description of a MyPlatform table's metadata.
  *
  * The `version` field is the key to CAS — your platform's underlying
  * store increments it on each update, so a publish can detect a
  * concurrent modification by reading the current version and
  * verifying it on commit.
  *
  * Per scala-data-driven-refacer §1: pure data, `Product with Serializable`. */
final case class MyPlatformTableMeta(
    table:     String,
    realmId:   String,
    version:   Long,
    active:    Boolean,
) extends Product with Serializable