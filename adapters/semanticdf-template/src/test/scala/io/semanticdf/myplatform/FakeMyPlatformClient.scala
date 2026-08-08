package io.semanticdf.myplatform

import io.semanticdf.core.engine.ResolvedSchema

import scala.collection.mutable

/** Test fixture: a hand-driven [[MyPlatformClient]] that returns scripted
  * responses.
  *
  * Mirrors `FakeUnityCatalogClient` (PR #424), `FakeHiveMetastoreClient`
  * (PR #423), and `FakeHeraClient` (PR #425).
  *
  * ==Why a Map (not a behavior mock)==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): the fake is a data table — it answers a deterministic
  * question with a deterministic answer.
  *
  * ==Why a `def empty` (not `val empty`)==
  *
  * Per the lesson from PR #423 (cross-test state pollution caused 6
  * spurious failures): using `def` ensures each test gets a fresh
  * un-polluted fake. CRITICAL for mutable fakes that support
  * publish-side behavior. */
class FakeMyPlatformClient(
    initialTables: Map[String, MyPlatformTableMeta] = Map.empty,
    initialResults: Map[String, MyPlatformResult] = Map.empty,
    initialRealms: Map[String, String]        = Map.empty,
) extends MyPlatformClient {

  // Mutable state — tests pre-populate, adapter tests mutate this.
  private val tables = mutable.Map.from(initialTables)
  private val results = mutable.Map.from(initialResults)
  // Catalog → realm mapping (per the template's resolver contract).
  private val realms = mutable.Map.from(initialRealms)

  // Recorded calls — for assertion in tests.
  val executedQueries: mutable.ListBuffer[(String, String, Int)] = mutable.ListBuffer.empty
  val describedTables: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty
  val getTableMetaCalls: mutable.ListBuffer[(String, String)] = mutable.ListBuffer.empty
  val createdTables: mutable.ListBuffer[(String, String, MyPlatformTableMeta)] = mutable.ListBuffer.empty
  val updatedTables: mutable.ListBuffer[(String, String, MyPlatformTableMeta, Long)] = mutable.ListBuffer.empty

  override def executeQuery(
      sql:     String,
      realmId: String,
      limit:   Int    = 100,
  ): Either[MyPlatformError, MyPlatformResult] = {
    executedQueries += ((sql, realmId, limit))
    results.get(sql) match {
      case Some(r) => Right(r)
      case None    => Right(MyPlatformResult(Nil, Nil))  // no scripted → empty
    }
  }

  override def describeTable(
      table:   String,
      realmId: String,
  ): Either[MyPlatformError, ResolvedSchema] = {
    describedTables += ((table, realmId))
    tables.get(s"$realmId:$table") match {
      case Some(_)  => Right(ResolvedSchema(Map("col" -> "varchar")))
      case None     => Left(MyPlatformError.NotFound(reason = s"table '$table' not found in realm $realmId"))
    }
  }

  override def getTableMeta(
      table:   String,
      realmId: String,
  ): Either[MyPlatformError, MyPlatformTableMeta] = {
    getTableMetaCalls += ((table, realmId))
    tables.get(s"$realmId:$table") match {
      case Some(meta) => Right(meta)
      case None       => Left(MyPlatformError.NotFound(reason = s"table '$table' not found in realm $realmId"))
    }
  }

  override def createTable(
      table:   String,
      realmId: String,
      meta:    MyPlatformTableMeta,
  ): Either[MyPlatformError, MyPlatformTableMeta] = {
    createdTables += ((table, realmId, meta))
    if (tables.contains(s"$realmId:$table")) {
      Left(MyPlatformError.AlreadyExists(reason = s"table '$table' already exists"))
    } else {
      val newMeta = meta.copy(version = 1L)
      tables += (s"$realmId:$table" -> newMeta)
      Right(newMeta)
    }
  }

  override def updateTable(
      table:           String,
      realmId:         String,
      meta:            MyPlatformTableMeta,
      expectedVersion: Long,
  ): Either[MyPlatformError, MyPlatformTableMeta] = {
    updatedTables += ((table, realmId, meta, expectedVersion))
    tables.get(s"$realmId:$table") match {
      case Some(existing) if existing.version == expectedVersion =>
        val newMeta = existing.copy(version = existing.version + 1)
        tables += (s"$realmId:$table" -> newMeta)
        Right(newMeta)
      case Some(_) =>
        Left(MyPlatformError.Conflict(reason = s"version mismatch for '$table'"))
      case None =>
        Left(MyPlatformError.NotFound(reason = s"table '$table' not found"))
    }
  }

  override def listTables(
      realmId: String,
      prefix:  String,
  ): Either[MyPlatformError, List[String]] = {
    val matching = tables.keys.collect {
      case key @ s"$realmId:$name" if name.startsWith(prefix) => name
    }.toList.sorted  // deterministic order per scala-data-driven-refacer §1
    Right(matching)
  }

  override def resolveRealmId(catalogName: String): Option[String] = {
    if (catalogName.isEmpty) None else realms.get(catalogName)
  }

  // Mutable: register a catalog → realm mapping. Returns `this` for fluent chaining.
  def addRealm(catalog: String, realm: String): FakeMyPlatformClient = {
    realms += (catalog -> realm)
    this
  }

  // -- inspection helpers --

  /** Snapshot the current version for a given table (for CAS assertions). */
  def currentVersion(table: String, realmId: String): Option[Long] =
    tables.get(s"$realmId:$table").map(_.version)
}

object FakeMyPlatformClient {

  /** Empty fake — every lookup returns the relevant "not found" /
    * empty result. `def` not `val` per the cross-test pollution lesson. */
  def empty: FakeMyPlatformClient = new FakeMyPlatformClient()

  /** Build a fake with the given tables. */
  def withTables(tables: (String, String, Long)*): FakeMyPlatformClient = {
    val m = tables.map { case (name, realmId, version) =>
      s"$realmId:$name" -> MyPlatformTableMeta(name, realmId, version, active = true)
    }.toMap
    new FakeMyPlatformClient(initialTables = m)
  }

  /** Build a fake that returns the given result for a specific SQL
    * string. */
  def withQueryResult(
      sql:    String,
      result: MyPlatformResult,
  ): FakeMyPlatformClient = new FakeMyPlatformClient(initialResults = Map(sql -> result))

  /** Build a fake with the given catalog → realm mappings. */
  def withRealms(realms: (String, String)*): FakeMyPlatformClient =
    new FakeMyPlatformClient(initialRealms = realms.toMap)
}