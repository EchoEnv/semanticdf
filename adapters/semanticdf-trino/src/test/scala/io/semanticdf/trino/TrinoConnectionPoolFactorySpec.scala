package io.semanticdf.trino

import com.zaxxer.hikari.HikariConfig
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 5: pin `TrinoConnectionPoolFactory` invariants.
  *
  * These tests verify the *configuration* (HikariConfig) and the
  * factory shape. They do NOT instantiate a real HikariDataSource
  * (which would try to connect to a real Trino cluster). The
  * actual round-trip is covered by the integration test
  * (`TrinoIntegrationSpec` from PR #384).
  *
  * ==Why config-only tests (not behavioral)==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): the *configuration* of a pool is pure
  * data. We can assert on it without doing IO. Instantiation
  * requires a real cluster, which we explicitly avoid here.
  *
  * Per debug-mantra §1: "reliable repro" — these tests are
  * deterministic (no timing, no IO), run in milliseconds, and
  * don't depend on Docker. */
class TrinoConnectionPoolFactorySpec extends AnyFunSuite with Matchers {

  // -- configuration propagation --

  test("maxPoolSize is propagated to HikariConfig") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl     = "jdbc:trino://test-cluster:8080",
      maxPoolSize = 7,
    )
    config.getMaximumPoolSize shouldBe 7
  }

  test("jdbcUrl is propagated to HikariConfig") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://my-cluster:1234",
    )
    config.getJdbcUrl shouldBe "jdbc:trino://my-cluster:1234"
  }

  test("user is propagated to HikariConfig") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://test:8080",
      user    = "alice",
    )
    config.getUsername shouldBe "alice"
  }

  test("poolName is propagated to HikariConfig") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl  = "jdbc:trino://test:8080",
      poolName = "my-custom-pool",
    )
    config.getPoolName shouldBe "my-custom-pool"
  }

  test("password is propagated when set") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl  = "jdbc:trino://test:8080",
      password = "secret",
    )
    config.getPassword shouldBe "secret"
  }

  test("password is null when not specified (no-auth default)") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://test:8080",
    )
    // Default: no password (Trino's no-auth mode).
    config.getPassword shouldBe null
  }

  // -- safety defaults (always set) --

  test("leak detection threshold is set to 60 seconds") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://test:8080",
    )
    config.getLeakDetectionThreshold shouldBe 60_000L
  }

  test("connection test query is 'SELECT 1'") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://test:8080",
    )
    // Trino supports 'SELECT 1' as a cheap ping. Using anything
    // heavier (e.g. queries against system.runtime.*) would add
    // 1-2ms to every borrowed-connection validation.
    config.getConnectionTestQuery shouldBe "SELECT 1"
  }

  // -- defaults --

  test("default maxPoolSize is 10") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://test:8080",
    )
    config.getMaximumPoolSize shouldBe 10
  }

  test("default user is 'test'") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://test:8080",
    )
    config.getUsername shouldBe "test"
  }

  test("default poolName is 'semanticdf-trino'") {
    val config = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://test:8080",
    )
    config.getPoolName shouldBe "semanticdf-trino"
  }

  // -- factory shape --
  //
  // Note: we DO NOT test `hikari(...)` directly here because it
  // eagerly instantiates a `HikariDataSource`, which in turn
  // tries to connect to the cluster (Hikari calls the validation
  // query at construction). That requires a running Trino
  // cluster — see `TrinoIntegrationSpec` (PR #384) for that.
  // The factory's *return type* is `() => TrinoConnection` per
  // the source code; Scala's type checker is the implicit test
  // that the public API contract is honored.

  test("each build returns an independent configuration") {
    // Per scala-data-driven-refactor §1: configuration is data;
    // each call to the factory must produce an independent config
    // (no shared mutable state across calls).
    val config1 = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://a:8080",
    )
    val config2 = TrinoConnectionPoolFactory.buildHikariConfig(
      jdbcUrl = "jdbc:trino://b:8080",
    )
    config1 should not be theSameInstanceAs(config2)
    config1.getJdbcUrl shouldBe "jdbc:trino://a:8080"
    config2.getJdbcUrl shouldBe "jdbc:trino://b:8080"
  }
}
