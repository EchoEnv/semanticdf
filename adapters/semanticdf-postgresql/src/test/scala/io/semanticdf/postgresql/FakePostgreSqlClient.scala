package io.semanticdf.postgresql

import io.semanticdf.core.engine.ResolvedSchema

import scala.collection.mutable

/** Test fixture: a hand-driven [[PostgreSqlClient]] that returns scripted
  * responses.
  *
  * Mirrors `FakeUnityCatalogClient` (PR #424), `FakeHiveMetastoreClient`
  * (PR #423), `FakeHeraClient` (PR #425), and the new template's
  * `FakeMyPlatformClient` (PR #426).
  *
  * ==Why a Map (not a behavior mock)==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior lives
  * elsewhere"): the fake is a data table — it answers a deterministic
  * question with a deterministic answer.
  *
  * ==Why `def empty` (not `val empty`)==
  *
  * Per the lesson from PR #423 (cross-test state pollution caused 6
  * spurious failures): use `def` for fresh state per test. */
class FakePostgreSqlClient(
    initialTables: Map[String, Map[String, String]] = Map.empty,  // table -> column -> type
    initialManifests: Map[String, (String, Long)]       = Map.empty,  // table -> (digest, xmin)
) extends PostgreSqlClient {

  // Mutable state — tests pre-populate, adapter tests mutate this.
  private val tables       = mutable.Map.from(initialTables)
  private val manifests    = mutable.Map.from(initialManifests)
  private val scriptedResults = mutable.Map.empty[String, PostgreSqlResult]

  // Recorded calls — for assertion in tests.
  val executedQueries: mutable.ListBuffer[(String, Seq[Any])] = mutable.ListBuffer.empty
  val createdTables:   mutable.ListBuffer[(String, String, List[PostgreSqlColumn])] = mutable.ListBuffer.empty
  val casUpdates:      mutable.ListBuffer[(String, String, Long, String)] = mutable.ListBuffer.empty

  override def executeQuery(
      sql:    String,
      params: Seq[Any] = Seq.empty,
  ): Either[PostgreSqlError, PostgreSqlResult] = {
    executedQueries += ((sql, params))
    scriptedResults.get(sql) match {
      case Some(r) => Right(r)
      case None    => Right(PostgreSqlResult(Nil, Nil))  // no scripted → empty
    }
  }

  override def describeTable(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, ResolvedSchema] = {
    val key = s"$schema.$table"
    tables.get(key) match {
      case Some(cols) => Right(ResolvedSchema(cols))
      case None       => Left(PostgreSqlError.TableNotFound(reason = s"table '$key' not found"))
    }
  }

  override def createTable(
      schema:  String,
      table:   String,
      columns: List[PostgreSqlColumn],
  ): Either[PostgreSqlError, Unit] = {
    val key = s"$schema.$table"
    createdTables += ((schema, table, columns))
    if (tables.contains(key)) {
      Left(PostgreSqlError.UniqueViolation(reason = s"table '$key' already exists"))
    } else {
      tables += (key -> columns.map(c => c.name -> c.dataType).toMap)
      Right(())
    }
  }

  override def dropTable(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, Unit] = {
    val key = s"$schema.$table"
    if (tables.contains(key)) {
      tables -= key
      manifests -= key
      Right(())
    } else {
      Left(PostgreSqlError.TableNotFound(reason = s"table '$key' not found"))
    }
  }

  override def getTableVersion(
      schema: String,
      table:  String,
  ): Either[PostgreSqlError, Long] = {
    val key = s"$schema.$table"
    manifests.get(key) match {
      case Some((_, xmin)) => Right(xmin)
      case None           => Left(PostgreSqlError.TableNotFound(reason = s"no manifest row in '$key'"))
    }
  }

  override def casUpdate(
      schema:         String,
      table:          String,
      expectedXmin:   Long,
      newContent:     String,
  ): Either[PostgreSqlError, Long] = {
    casUpdates += ((schema, table, expectedXmin, newContent))
    val key = s"$schema.$table"
    manifests.get(key) match {
      case Some((_, currentXmin)) if currentXmin == expectedXmin =>
        val newXmin = expectedXmin + 1
        manifests += (key -> (newContent, newXmin))
        Right(newXmin)
      case Some(_) =>
        Left(PostgreSqlError.CasConflict(reason = s"xmin $expectedXmin no longer current in '$key'"))
      case None =>
        // No manifest row yet → INSERT (treat as casUpdate creating the row)
        val newXmin = expectedXmin + 1
        manifests += (key -> (newContent, newXmin))
        Right(newXmin)
    }
  }

  // -- inspection helpers --

  /** Snapshot the current xmin for a given table. */
  def currentXmin(schema: String, table: String): Option[Long] =
    manifests.get(s"$schema.$table").map(_._2)

  /** Pre-populate a scripted result for a specific SQL string. */
  def withQueryResult(sql: String, result: PostgreSqlResult): FakePostgreSqlClient = {
    scriptedResults += (sql -> result)
    this
  }
}

object FakePostgreSqlClient {

  /** Empty fake — every lookup returns the relevant "not found" /
    * empty result. `def` not `val` per the cross-test pollution lesson. */
  def empty: FakePostgreSqlClient = new FakePostgreSqlClient()

  /** Build a fake with the given tables. */
  def withTables(tables: List[(String, String, Map[String, String])]): FakePostgreSqlClient = {
    val m = tables.map { case (schema, table, cols) =>
      s"$schema.$table" -> cols
    }.toMap
    new FakePostgreSqlClient(initialTables = m)
  }

  /** Build a fake with the given manifests (for catalog adapter tests). */
  def withManifests(manifests: List[(String, String, String, Long)]): FakePostgreSqlClient = {
    val m = manifests.map { case (schema, table, digest, xmin) =>
      s"$schema.$table" -> (digest -> xmin)
    }.toMap
    new FakePostgreSqlClient(initialManifests = m)
  }
}
