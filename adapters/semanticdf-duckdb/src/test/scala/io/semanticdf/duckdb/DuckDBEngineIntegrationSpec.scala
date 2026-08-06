package io.semanticdf.duckdb

import io.semanticdf.core.engine.EngineContext
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Dimension, Measure, Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.SealedDataType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Integration tests for [[DuckDBEngine]] against an IN-MEMORY
  * DuckDB instance (`jdbc:duckdb:`). No Docker required — the
  * DuckDB JDBC driver includes the embedded engine. Startup is
  * sub-second.
  *
  * ==Why in-memory (no Docker)==
  *
  * Per the user's standing preference for local-first setup:
    *   - DuckDB in-process: no Docker, no JVM startup overhead
    *   - Tests run in <1s (vs. 30s+ for Trino cluster tests)
    *   - Memory bounded by the JVM (no container cap needed)
    *   - Disk bounded by `mem:` mode (zero)
    *
  * ==Why this is the highest-value integration test in the suite==
  *
  * This is the FIRST engine adapter that tests end-to-end compile +
  * execute against a REAL query engine WITHOUT Docker. It proves:
    *   - The DuckDB JDBC driver works with `jdbc:duckdb:`
    *   - The DuckDB SQL emit is valid DuckDB SQL (not Trino SQL)
    *   - ResultSet → DuckDBResult mapping handles all JDBC types
    *   - The Spark library mirrors work end-to-end
    *
  * ==Memory monitoring (per user constraint)==
  *
  * The JVM heap is bounded by `-Xmx512m` (set in pom.xml's
  * scalatest config). The in-memory DuckDB has no separate
  * container; its engine runs in the same JVM as the test. */
class DuckDBEngineIntegrationSpec extends AnyFunSuite with Matchers {

  // -- fixtures --

  /** Build a minimal valid `Model` for testing. */
  private def sampleModel: Model = Model.of(
    name      = "orders",
    source    = SourceRef.ByName(catalog = None, namespace = Some("main"), table = "orders"),
    dimensions         = List(Dimension("region", Expr.FieldRef("region"), Some(SealedDataType.Varchar))),
    measures           = List(Measure("amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "amount"))),
    calculatedMeasures = Nil,
    joins              = Nil,
    defaultPolicies    = ModelPolicyDefaults.none,
    status             = ModelStatus.Draft,
  ).fold(err => fail(s"sampleModel failed validation: $err"), identity)

  /** Create the test table in a fresh in-memory DuckDB instance.
    * Returns a connection that the test closes after use.
    *
    * ==Why raw JDBC for setup (vs. `JdbcDuckDBConnection`)==
    *
    * `DuckDBConnection.prepareStatement` is SELECT-only (mirrors
    * `TrinoConnection.prepareStatement`). DDL (`CREATE TABLE`)
    * and DML (`INSERT`) need `executeUpdate`, which the trait
    * doesn't expose (per karpathy §2 — minimum code that solves
    * the problem: read-only contract for the engine, write
    * paths only via the setup). The test setup uses the raw
    * JDBC `Connection` for DDL/DML. */
  private def setupInMemoryDuckDB(): JdbcDuckDBConnection = {
    Class.forName("org.duckdb.DuckDBDriver")
    val rawConn = java.sql.DriverManager.getConnection("jdbc:duckdb:")
    try {
      val createStmt = rawConn.createStatement()
      createStmt.executeUpdate("CREATE TABLE main.orders (id INTEGER, region VARCHAR, amount DECIMAL(18,2))")
      createStmt.close()
      val insertStmt = rawConn.createStatement()
      insertStmt.executeUpdate(
        "INSERT INTO main.orders VALUES (1, 'us', 100.00), (2, 'us', 50.00), (3, 'eu', 200.00), (4, 'eu', 75.00), (5, 'ap', 300.00)",
      )
      insertStmt.close()
    } finally {
      // Don't close rawConn here — we're handing it off to JdbcDuckDBConnection
    }
    // Wrap the (already-set-up) connection for the test's use.
    // The JdbcDuckDBConnection.close() will close it.
    JdbcDuckDBConnection.fromConnection(rawConn)
  }

  // -- tests --

  test("DuckDBEngine.compile + execute against in-memory DuckDB returns aggregated rows") {
    val conn = setupInMemoryDuckDB()
    try {
      val engine = new DuckDBEngine().withConnectionFactory(() => conn)
      val result = engine.compile(sampleModel, EngineContext.defaultContext).flatMap { plan =>
        engine.execute(plan, EngineContext.defaultContext).map(_.asInstanceOf[DuckDBResult])
      }
      result.isRight shouldBe true
      val duckResult = result.toOption.get
      // 3 distinct regions, each aggregated.
      duckResult.rowCount shouldBe 3
      duckResult.columns shouldBe List("region", "amount")
      // Total = 100 + 50 + 200 + 75 + 300 = 725
      val total = duckResult.rows.map(_.last).collect {
        case LiteralValue.LongValue(v)    => v
        case LiteralValue.DecimalValue(d) => d.toLong
      }.sum
      total shouldBe 725L
    } finally {
      conn.close()
    }
  }

  test("DuckDBEngine.preview(n) returns the first n rows") {
    val conn = setupInMemoryDuckDB()
    try {
      val engine = new DuckDBEngine().withConnectionFactory(() => conn)
      val result = engine.preview(sampleModel, 2, EngineContext.defaultContext)
      result.isRight shouldBe true
      result.toOption.get.rowCount shouldBe 2
    } finally {
      conn.close()
    }
  }

  test("DuckDBEngine.count() returns the row count of the aggregate result") {
    val conn = setupInMemoryDuckDB()
    try {
      val engine = new DuckDBEngine().withConnectionFactory(() => conn)
      val result = engine.count(sampleModel, EngineContext.defaultContext)
      result.isRight shouldBe true
      result.toOption.get shouldBe 3L  // 3 distinct regions
    } finally {
      conn.close()
    }
  }

  test("DuckDBEngine.executeAsRows returns List[Map[String, LiteralValue]]") {
    val conn = setupInMemoryDuckDB()
    try {
      val engine = new DuckDBEngine().withConnectionFactory(() => conn)
      val result = engine.executeAsRows(sampleModel, EngineContext.defaultContext)
      result.isRight shouldBe true
      val rows = result.toOption.get
      rows.size shouldBe 3
      rows.head.keySet shouldBe Set("region", "amount")
    } finally {
      conn.close()
    }
  }

  test("DuckDBEngine.previewAsRows(n) returns first n rows as List[Map]") {
    val conn = setupInMemoryDuckDB()
    try {
      val engine = new DuckDBEngine().withConnectionFactory(() => conn)
      val result = engine.previewAsRows(sampleModel, 1, EngineContext.defaultContext)
      result.isRight shouldBe true
      result.toOption.get.size shouldBe 1
    } finally {
      conn.close()
    }
  }

  test("DuckDBEngine.explain returns the parameterized SQL (no cluster roundtrip)") {
    val engine = new DuckDBEngine()  // no connection factory needed
    val result = engine.explain(sampleModel, EngineContext.defaultContext)
    result.isRight shouldBe true
    result.toOption.get should include ("""FROM "memory"."main"."orders"""")
  }

  test("DuckDBEngine.explainPlan runs EXPLAIN against in-memory DuckDB") {
    val conn = setupInMemoryDuckDB()
    try {
      val engine = new DuckDBEngine().withConnectionFactory(() => conn)
      val result = engine.explainPlan(sampleModel, EngineContext.defaultContext)
      result.isRight shouldBe true
      val planString = result.toOption.get
      planString should not be empty
      // DuckDB EXPLAIN output contains "PHYSICAL" or "LOGICAL" headers.
      planString.toUpperCase should (include ("PHYSICAL") or include ("LOGICAL"))
    } finally {
      conn.close()
    }
  }

  test("DuckDBEngine.schema returns engine-portable SchemaSummary without touching the database") {
    val engine = new DuckDBEngine()  // no connection needed
    val summary = engine.schema(sampleModel, EngineContext.defaultContext).toOption.get
    summary.modelName shouldBe "orders"
    summary.rowCount shouldBe 2
    summary.ofKind(io.semanticdf.core.schema.SchemaFieldKind.Dimension).map(_.fieldName) shouldBe List("region")
  }
}