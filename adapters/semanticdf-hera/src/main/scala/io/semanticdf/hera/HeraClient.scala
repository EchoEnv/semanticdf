package io.semanticdf.hera

import io.semanticdf.core.engine.ResolvedSchema
import io.semanticdf.core.schema.SchemaSummary

/** Boundary trait for Hera data-plane operations.
  *
  * Mirrors the `UnityCatalogClient` pattern (PR #394 / #424): the
  * SHAPE of the contract is here; the BODY (the actual HTTP calls)
  * lives in the concrete impl. Tests inject a fake via this trait
  * (no real Hera instance required — per user constraint: "not yet
  * need to provision").
  *
  * ==Why a separate trait from `HeraAuth`==
  *
  * Per scala-data-driven-refacer §1: auth and data-plane are
  * separate concerns. `HeraAuth` owns the OAuth2 token lifecycle;
  * `HeraClient` consumes the token (via the `Authorization` header)
  * and performs the actual data work. Splitting them lets:
  *
  *   - Tokens be cached / refreshed independently of data ops
  *   - `HeraAuth` be reused by callers that need only auth (e.g.
  *     `HeraCatalogAdapter` doesn't need to re-login on every publish)
  *   - Tests fake each layer independently
  *
  * ==Why all data-plane methods return `Either[HeraClientError, X]`==
  *
  * Per `docs/design/error-handling-style.md`: public APIs must return
  * `Either[L, X]` where `L` is a sealed ADT. [[HeraClientError]] is
  * the ADT; it has 11 SPECIFIC failure cases (no catch-all
  * `ServerError`) so callers can distinguish network from auth from
  * query-syntax from CAS-conflict from engine-failure.
  *
  * ==Why no `getTableProperties` (vs UC's pattern)==
  *
  * The UC adapter reads `properties` map for CAS (PR #424). Hera's
  * TableManage has a different CAS story: `optLock` is a version
  * field on the table that increments on each update. We use that
  * for CAS via the `getTable(optLock)` / `update(optLock)`
  * round-trip — not via a separate "get properties" call. The CAS
  * surface lives in [[HeraCatalogAdapter]], not here.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-hera/src/main/scala/io/semanticdf/hera/HeraClient.scala` */
trait HeraClient extends Serializable {

  // -- Query / Describe (Engine surface) --

  /** Execute a SQL query against Hera (sync mode). Maps to
    * `POST /private/explore/query`.
    *
    * Per error-handling-style.md: returns `Either[HeraClientError, X]`.
    * Failures can be: 401/602 (auth), 600 (query syntax), 521 (engine
    * broken), or transport failure.
    *
    * ==Zeus (execution engine)==
    *
    * Per user domain knowledge: each Hera realm hosts one or more
    * "Zeus" execution engines (e.g. a Trino-backed Zeus and a
    * Spark-backed Zeus in the same realm). The query must specify
    * which Zeus to run on. `zeusId: None` means "let the server pick
    * the default for this realm" (Hera will use the
    * `oauthUser.moduleZeusList[].default` entry).
    *
    * @param sql       the SQL string to execute
    * @param realmId   the realm scope for the query
    * @param limit     max rows to return (Hera enforces this server-side)
    * @param jobGroupId optional job group id for async / batch tracking
    * @param zeusId    the Zeus engine id to run on. `None` = server default. */
  def executeQuery(
      sql:        String,
      realmId:    Long,
      limit:      Int           = 100,
      jobGroupId: Option[String] = None,
      zeusId:     Option[Long]  = None,
  ): Either[HeraClientError, HeraQueryResult]

  /** Describe a table's schema. Maps to
    * `POST /private/explore/describe/table`.
    *
    * Per error-handling-style.md "may not exist" rule: this is a
    * computation (HTTP call), NOT a pure lookup — so it returns
    * `Either[HeraClientError, ResolvedSchema]`, not `Option[...]`.
    * The 404 case is encoded as `Left(HeraClientError.NotFound)`.
    */
  def describeTable(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, ResolvedSchema]

  /** Register a Spark job group for async query tracking. Maps to
    * `POST /private/sparkjobmanage/job/register`.
    *
    * Per error-handling-style.md: returns `Either[HeraClientError, String]`
    * where the String is the `jobGroupId` returned by Hera. */
  def registerSparkJob(
      action:  String,
      realmId: Long,
  ): Either[HeraClientError, String]

  // -- TableManage (Catalog surface) --

  /** List tables in a realm, optionally filtered by a name prefix.
    * Returns `Right(Nil)` if the realm has no tables or doesn't exist.
    *
    * Per error-handling-style.md: returns
    * `Either[HeraClientError, List[String]]` (typed failure ADT).
    *
    * Note: Hera's documented API surface doesn't expose a direct
    * "list tables" endpoint we have access to. For v1, this
    * returns the empty list — callers needing table enumeration
    * should use the resolve-via-`describeTable` loop or wait for
    * v0.4.0 when we wire up a dedicated list endpoint. Documented
    * limitation per the existing UC/HMS patterns. */
  def listTables(
      realmId: Long,
      prefix:  String,
  ): Either[HeraClientError, List[String]]

  /** Check whether a table exists in the given realm. Maps to
    * `POST /private/table/manage/isExists`.
    *
    * Returns `Either[HeraClientError, Boolean]` per the standard.
    * 401/403 → typed error cases. Transport failure → `NetworkError`. */
  def tableExists(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, Boolean]

  /** Get a table's metadata including the `optLock` version field
    * (used by [[HeraCatalogAdapter]] for CAS).
    *
    * Returns `Either[HeraClientError, HeraTableMeta]`. The metadata
    * is what the catalog adapter uses to verify the expected
    * digest on `CompareAndSet` publish.
    *
    * Returns `Left(HeraClientError.NotFound)` if the table doesn't exist. */
  def getTableMeta(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, HeraTableMeta]

  /** Create a new table from a SQL definition. Maps to
    * `POST /private/table/manage/createTableFromSql`.
    *
    * Per error-handling-style.md: `Left(HeraClientError.AlreadyExists)`
    * on duplicate. `Left(HeraClientError.BadRequest)` on invalid SQL. */
  def createTableFromSql(
      tableName: String,
      dataType:  String,
      sql:       String,
      realmId:   Long,
  ): Either[HeraClientError, HeraTableMeta]

  /** Update a table's source data (the path / SQL behind the table).
    * Maps to `POST /private/table/manage/update`.
    *
    * Per error-handling-style.md: returns
    * `Left(HeraClientError.Conflict)` if `expectedOptLock` doesn't
    * match the current value (CAS failure). */
  def updateTableSource(
      tableName:      String,
      path:           String,
      expectedOptLock: Long,
      realmId:        Long,
  ): Either[HeraClientError, HeraTableMeta]

  /** Refresh a table (re-derive the table's contents). Maps to
    * `POST /private/table/manage/refresh`.
    *
    * Per error-handling-style.md: `Left(HeraClientError.NotFound)` if
    * the table doesn't exist. */
  def refreshTable(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, Unit]

  // -- RealmManage (tenancy surface) --

  /** List all active realms. Maps to `POST /private/realm/list`.
    *
    * Per error-handling-style.md: returns
    * `Either[HeraClientError, List[HeraRealm]]`. */
  def listRealms(): Either[HeraClientError, List[HeraRealm]]

  /** Get a realm by id. Maps to `POST /private/realm/get/{id}/full`
    * (or `GET /private/realm/get/{id}` for the bare metadata).
    *
    * Returns `Left(HeraClientError.NotFound)` if no realm exists
    * at that id. */
  def getRealm(realmId: Long): Either[HeraClientError, Option[HeraRealm]]
}

/** Engine-portable description of a Hera query result.
  *
  * Per scala-data-driven-refacer §1: pure data. `extends Product with
  * Serializable` per the distributed-serialization reference (the
  * result rows may flow through a Restate boundary in the platform
  * mode).
  *
  * Mirrors the shape of Trino / JDBC result sets: a list of fields
  * (column metadata) + a list of rows (the actual values). */
final case class HeraQueryResult(
    fields: List[HeraField],
    rows:   List[Map[String, Any]],
    queryTime: java.time.Duration,
) extends Product with Serializable

/** Engine-portable description of a Hera table column. */
final case class HeraField(
    name:     String,
    dataType: String,
    nullable: Boolean,
    comment:  Option[String] = None,
) extends Product with Serializable

/** Engine-portable description of a Hera table's metadata.
  *
  * The `optLock` field is the key to CAS — Hera increments it on
  * each update, so a publish can detect a concurrent modification
  * by reading the current `optLock` and verifying it on commit.
  *
  * Per scala-data-driven-refacer §1: pure data; `Product with
  * Serializable` per the distributed-serialization reference. */
final case class HeraTableMeta(
    tableName: String,
    realmId:   Long,
    optLock:   Long,
    active:    Boolean,
    dataType:  Option[String] = None,
) extends Product with Serializable

/** Engine-portable description of a Hera realm.
  *
  * Per scala-data-driven-refacer §1: pure data; `Product with
  * Serializable`. Carries the minimum the catalog / engine need to
  * route per-tenant — full realm config (metastoreConf, datastoreConf,
  * etc.) is excluded to keep the ADT lean. */
final case class HeraRealm(
    id:          Long,
    name:        String,
    description: Option[String] = None,
    active:      Boolean       = true,
) extends Product with Serializable