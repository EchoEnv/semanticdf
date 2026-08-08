package io.semanticdf.hera

import io.semanticdf.core.engine.ResolvedSchema

import scala.collection.mutable

/** Test fixture: a hand-driven [[HeraClient]] that returns scripted
  * responses.
  *
  * Mirrors `FakeUnityCatalogClient` (PR #424) and
  * `FakeHiveMetastoreClient` (PR #423): tests register scripted
  * responses; the fake returns them deterministically. No stubs,
  * no spies, no method-call recording. Tests inspect the
  * adapter's behavior, not the client's behavior.
  *
  * ==Why a Map (not a behavior mock)==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): the fake is a data table — it answers a deterministic
  * question with a deterministic answer.
  *
  * ==Why a `def empty` (not `val empty`)==
  *
  * Per the lesson from PR #423: cross-test state pollution caused 6
  * spurious failures. Using `def` ensures each test gets a fresh
  * un-polluted fake. */
final class FakeHeraClient(
    initialTables: Map[String, HeraTableMeta] = Map.empty,
    initialRealms: List[HeraRealm]           = Nil,
    initialQueryResults: Map[String, HeraQueryResult] = Map.empty,
) extends HeraClient {

  // Mutable state — tests can pre-populate, and adapter tests
  // exercise publish / discover / list paths that mutate this.
  private val tables: mutable.Map[String, HeraTableMeta] =
    mutable.Map.from(initialTables)
  private val realms: mutable.ListBuffer[HeraRealm] =
    mutable.ListBuffer.from(initialRealms)
  private val queryResults: mutable.Map[String, HeraQueryResult] =
    mutable.Map.from(initialQueryResults)

  // Recorded calls — for assertion in tests.
  val executedQueries: mutable.ListBuffer[(String, Long, Int, Option[String], Option[Long])] = mutable.ListBuffer.empty
  val describedTables: mutable.ListBuffer[(String, Long)] = mutable.ListBuffer.empty
  val tableExistsChecks: mutable.ListBuffer[(String, Long)] = mutable.ListBuffer.empty
  val getTableMetaCalls: mutable.ListBuffer[(String, Long)] = mutable.ListBuffer.empty
  val createdTables: mutable.ListBuffer[(String, String, String, Long)] = mutable.ListBuffer.empty
  val updatedTables: mutable.ListBuffer[(String, String, Long, Long)] = mutable.ListBuffer.empty
  val refreshedTables: mutable.ListBuffer[(String, Long)] = mutable.ListBuffer.empty

  // -- Query / Describe --

  override def executeQuery(
      sql:        String,
      realmId:    Long,
      limit:      Int           = 100,
      jobGroupId: Option[String] = None,
      zeusId:     Option[Long]  = None,
  ): Either[HeraClientError, HeraQueryResult] = {
    executedQueries += ((sql, realmId, limit, jobGroupId, zeusId))
    // Per the test helper contract: match by SQL substring.
    queryResults.find { case (key, _) => sql.contains(key) } match {
      case Some((_, r)) => Right(r)
      case None         =>
        // No scripted result — return empty. Real tests should
        // pre-populate via withQueryResult.
        Right(HeraQueryResult(Nil, Nil, java.time.Duration.ZERO))
    }
  }

  override def describeTable(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, ResolvedSchema] = {
    describedTables += ((tableName, realmId))
    tables.get(s"$realmId:$tableName") match {
      case Some(meta) =>
        // Synthesize a minimal schema (1 column) from the meta.
        // Real tests should override via a richer fixture.
        Right(ResolvedSchema(Map("col" -> "varchar")))
      case None =>
        Left(HeraClientError.NotFound(reason = s"table '$tableName' not found in realm $realmId"))
    }
  }

  override def registerSparkJob(
      action:  String,
      realmId: Long,
  ): Either[HeraClientError, String] = {
    Right(s"jobgroup-$realmId-${System.nanoTime()}")
  }

  // -- TableManage --

  override def listTables(
      realmId: Long,
      prefix:  String,
  ): Either[HeraClientError, List[String]] = {
    val all = tables.keys.collect {
      case key @ s"$realmId:$name" if name.startsWith(prefix) => name
    }.toList.sorted
    Right(all)
  }

  override def tableExists(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, Boolean] = {
    tableExistsChecks += ((tableName, realmId))
    Right(tables.contains(s"$realmId:$tableName"))
  }

  override def getTableMeta(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, HeraTableMeta] = {
    getTableMetaCalls += ((tableName, realmId))
    tables.get(s"$realmId:$tableName") match {
      case Some(meta) => Right(meta)
      case None       => Left(HeraClientError.NotFound(reason = s"table '$tableName' not found in realm $realmId"))
    }
  }

  override def createTableFromSql(
      tableName: String,
      dataType:  String,
      sql:       String,
      realmId:   Long,
  ): Either[HeraClientError, HeraTableMeta] = {
    createdTables += ((tableName, dataType, sql, realmId))
    if (tables.contains(s"$realmId:$tableName")) {
      Left(HeraClientError.AlreadyExists(reason = s"table '$tableName' already exists in realm $realmId"))
    } else {
      val meta = HeraTableMeta(tableName, realmId, optLock = 1L, active = true, dataType = Some(dataType))
      tables += (s"$realmId:$tableName" -> meta)
      Right(meta)
    }
  }

  override def updateTableSource(
      tableName:      String,
      path:           String,
      expectedOptLock: Long,
      realmId:        Long,
  ): Either[HeraClientError, HeraTableMeta] = {
    updatedTables += ((tableName, path, expectedOptLock, realmId))
    tables.get(s"$realmId:$tableName") match {
      case Some(existing) if existing.optLock == expectedOptLock =>
        val newMeta = existing.copy(optLock = existing.optLock + 1)
        tables += (s"$realmId:$tableName" -> newMeta)
        Right(newMeta)
      case Some(_) =>
        Left(HeraClientError.Conflict(reason = s"optLock mismatch for '$tableName'"))
      case None =>
        Left(HeraClientError.NotFound(reason = s"table '$tableName' not found in realm $realmId"))
    }
  }

  override def refreshTable(
      tableName: String,
      realmId:   Long,
  ): Either[HeraClientError, Unit] = {
    refreshedTables += ((tableName, realmId))
    if (tables.contains(s"$realmId:$tableName")) Right(())
    else Left(HeraClientError.NotFound(reason = s"table '$tableName' not found in realm $realmId"))
  }

  // -- RealmManage --

  override def listRealms(): Either[HeraClientError, List[HeraRealm]] = {
    Right(realms.toList.sortBy(_.id))  // deterministic per scala-data-driven-refacer §1
  }

  override def getRealm(realmId: Long): Either[HeraClientError, Option[HeraRealm]] = {
    Right(realms.find(_.id == realmId))
  }

  // -- Inspection helpers --

  /** Snapshot the current optLock for a given table (for CAS assertions). */
  def currentOptLock(tableName: String, realmId: Long): Option[Long] =
    tables.get(s"$realmId:$tableName").map(_.optLock)

  /** Mutable: add a realm to the fake. Returns `this` for fluent
    * chaining with [[withTables]] (both are factory methods on the
    * companion; this is an instance method for chaining after the
    * factory call). */
  def addRealm(realm: HeraRealm): FakeHeraClient = {
    realms += realm
    this
  }
}

object FakeHeraClient {

  /** Empty fake — every lookup returns `None`. `def` not `val` per the
    * cross-test pollution lesson (PR #423). */
  def empty: FakeHeraClient = new FakeHeraClient()

  /** Build a fake with the given tables. */
  def withTables(tables: (String, Long, Long)*): FakeHeraClient = {
    val m = tables.map { case (name, realmId, optLock) =>
      s"$realmId:$name" -> HeraTableMeta(name, realmId, optLock, active = true)
    }.toMap
    new FakeHeraClient(initialTables = m)
  }

  /** Build a fake with the given realms. */
  def withRealms(realms: (Long, String)*): FakeHeraClient = {
    val r = realms.map { case (id, name) => HeraRealm(id, name) }.toList
    new FakeHeraClient(initialRealms = r)
  }

  /** Build a fake that returns the given [[HeraQueryResult]] when
    * an executeQuery call comes in. The SQL parameter is used as a
    * `contains` match (so the fake fires regardless of which exact
    * SQL the engine compiles — useful for tests that don't care
    * about exact SQL match, only about the result). */
  def withQueryResult(
      sql:    String,
      result: HeraQueryResult,
  ): FakeHeraClient = new FakeHeraClient(initialQueryResults = Map(sql -> result))
}
