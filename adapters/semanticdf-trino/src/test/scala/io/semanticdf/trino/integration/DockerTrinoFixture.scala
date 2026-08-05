package io.semanticdf.trino.integration

import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, DriverManager, Statement}

/** Base fixture for integration tests that talk to a real Trino
  * cluster via JDBC.
  *
  * Tests gated by `-Ddocker.tests=true` (ScalaTest system property).
  * Default mvn invocations skip these (developer machines without
  * Docker stay green).
  *
  * ==Why per-test `assume()` (not `beforeAll.cancel()`)==
  *
  * ScalaTest's `cancel()` aborts the whole suite on first call,
  * which Maven Surefire counts as a *build failure*. We instead
  * use `assume()` inside each test: when the gate is closed, each
  * test is reported as "cancelled" but the suite completes cleanly,
  * and the build succeeds. This keeps `mvn test` green on machines
  * without Docker.
  *
  * ==Why JDBC (not the TrinoEngine + withConnectionFactory wiring)==
  *
  * The bootstrap DDL + INSERT runs at *test setup* time, before any
  * `TrinoEngine` is constructed. Going through the production wiring
  * (with FakeTrinoConnection etc.) at bootstrap is unnecessary
  * indirection. JDBC is the lowest-friction way to populate the
  * test table.
  *
  * ==Why the strict resource caps matter for integration testing==
  *
  * Per user constraint: 'monitor memory, disk while running, to
  * not explode server.' The Docker caps (1.5GB hard, -Xmx768m JVM)
  * mean an accidentally-huge test query will OOM-kill the cluster
  * (predictable failure) rather than starving the host (unbounded
  * failure). The integration tests therefore exercise the *exact*
  * memory envelope users will get in production. */
abstract class DockerTrinoFixture
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  /** The integration-test gate. Set `-Ddocker.tests=true` to enable.
    * Read inside `assumeDocker()` so it sees the JVM's current
    * system properties (Surefire may fork + set system properties
    * after test-class construction).
    *
    * Note: we use `Boolean.parseBoolean` rather than `String.toBoolean`
    * because Maven's `<systemProperties>${docker.tests}</...>` substitutes
    * to the literal string "null" when the property is unset — Scala's
    * `toBoolean` throws on non-"true"/"false" inputs. `parseBoolean`
    * is lenient: any non-"true" string returns `false`. */
  private def dockerTestsEnabled: Boolean =
    java.lang.Boolean.parseBoolean(sys.props.getOrElse("docker.tests", "false"))

  /** The Trino cluster connection string. Override with `-Dtrino.url=...`. */
  protected val trinoUrl: String =
    sys.props.getOrElse(
      "trino.url",
      "jdbc:trino://localhost:8088",  // host port (mapped from container :8080)
    )

  /** Wrap a test body in this gate. If `-Ddocker.tests=true` is
    * not set, the test is cancelled (not failed) — Maven `mvn test`
    * stays green on dev machines without Docker. */
  protected def assumeDocker(): Assertion = {
    assume(dockerTestsEnabled, s"docker tests disabled — set -Ddocker.tests=true to enable (sys.props has docker.tests=${sys.props.get("docker.tests")}, trino.url=${sys.props.get("trino.url")})")
  }

  /** Open a fresh JDBC connection to Trino. Caller responsible
    * for closing. Throws if no cluster is reachable. */
  protected def openTrinoConnection(): Connection = {
    Class.forName("io.trino.jdbc.TrinoDriver")
    val conn = DriverManager.getConnection(
      s"$trinoUrl/memory", "test", null,
    )
    conn
  }

  /** Run a single SQL statement (DDL or DML) on a fresh connection. */
  protected def runSql(sql: String): Unit = {
    val conn = openTrinoConnection()
    try {
      usingStatement(conn) { stmt =>
        stmt.execute(sql)
      }
    } finally {
      conn.close()
    }
  }

  private def usingStatement[A](conn: Connection)(f: Statement => A): A = {
    val stmt = conn.createStatement()
    try f(stmt) finally stmt.close()
  }

  override def afterAll(): Unit = {
    // Drops created by the test are intentionally left in place —
    // teardown.sh wipes ./data/ which removes Trino's persistence.
    // Tests are self-cleaning on the next `docker compose restart`.
  }
}
