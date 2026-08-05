package io.semanticdf.trino

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

/** Factory for [[TrinoConnection]]-producing closures backed by
  * connection pools. Currently supports HikariCP (the de-facto
  * Java connection pool); future pools (Apache DBCP, Trino's
  * built-in pool) would add new factory methods.
  *
  * ==Why this exists==
  *
  * Per the README's open item #2, the default `JdbcTrinoConnection`
  * opens a fresh JDBC connection per `engine.execute()` (~5ms each).
  * For production throughput, connections should be pooled so
  * cluster consumers don't pay the TCP-handshake cost on every
  * query.
  *
  * ==Why HikariCP specifically==
  *
  *   - **Mature, well-tested**: industry-default Java connection pool
  *   - **Small footprint**: ~150 KB, no transitive bloat
  *   - **Test-friendly**: Hikari `DataSource` is mockable
  *   - **Tunable**: maximum pool size, leak detection, validation timeout
  *
  * ==Why an OBJECT (not a class)==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): the factory methods are pure functions
  * (`hikari(jdbcUrl) -> () => TrinoConnection`). No state, no
  * fields. An `object` is the canonical Scala home for pure
  * functions; a `class` would imply construction with state,
  * which we don't have.
  *
  * ==Why TWO methods (`buildHikariConfig` + `hikari`)==
  *
  * `hikari(...)` returns the connection factory. `buildHikariConfig(...)`
  * returns the configuration that the factory uses. Splitting
  * them allows:
  *   1. Tests to assert on the configuration without instantiating
  *      a real `HikariDataSource` (which would try to connect
  *      to a real Trino cluster).
  *   2. Advanced users to add custom Hikari properties (timeout,
  *      metrics, etc.) before creating the dataSource.
  *
  * ==Why `extends Serializable` is NOT on the factory==
  *
  * The factory closures hold a `HikariDataSource` (driver-local).
  * They must NOT cross serialization boundaries (cluster mode,
  * MCP wire format, future Restate journal). Per the design's
  * "closure-bypass" pattern, the engine's `connectionFactory` is
  * driver-local; factory closures are NOT serialized.
  *
  * ==Usage==
  *
  * {{{
  *   val pool = TrinoConnectionPoolFactory.hikari(
  *     jdbcUrl = "jdbc:trino://coordinator.example.com:8080",
  *     maxPoolSize = 10,
  *   )
  *   val engine = TrinoEngine.instance.withConnectionFactory(pool)
  * }}} */
object TrinoConnectionPoolFactory {

  /** Build a `HikariConfig` with our standard safety defaults.
    * Exposed publicly so tests can inspect configuration without
    * instantiating a real `HikariDataSource`.
    *
    * @param jdbcUrl     the Trino JDBC URL
    * @param maxPoolSize maximum connections (default 10)
    * @param user        JDBC user (default `"test"`)
    * @param password    JDBC password (default `null` — Trino's
    *                    no-auth mode; for password auth, set
    *                    SSL on the dataSource separately)
    * @param poolName    Hikari-internal pool name (for logs / JMX)
    * @return a `HikariConfig` ready to be wrapped in
    *         `HikariDataSource`
    */
  def buildHikariConfig(
      jdbcUrl:     String,
      maxPoolSize: Int    = 10,
      user:        String = "test",
      password:    String = null,
      poolName:    String = "semanticdf-trino",
  ): HikariConfig = {
    val config = new HikariConfig()
    config.setJdbcUrl(jdbcUrl)
    config.setUsername(user)
    if (password != null) config.setPassword(password)
    config.setMaximumPoolSize(maxPoolSize)
    config.setPoolName(poolName)
    // Leak-detection: log a stack trace if a borrowed connection
    // isn't returned within 60s. Helps catch forgotten close().
    config.setLeakDetectionThreshold(60_000L)
    // Connection validation query. Trino supports "SELECT 1"
    // as a cheap ping.
    config.setConnectionTestQuery("SELECT 1")
    config
  }

  /** Build a `() => TrinoConnection` factory backed by HikariCP.
    *
    * @return a factory function that borrows a connection from
    *         the pool on each call; the returned `TrinoConnection`
    *         returns its connection to the pool on `close()`
    */
  def hikari(
      jdbcUrl:     String,
      maxPoolSize: Int    = 10,
      user:        String = "test",
      password:    String = null,
      poolName:    String = "semanticdf-trino",
  ): () => TrinoConnection = {
    val config   = buildHikariConfig(jdbcUrl, maxPoolSize, user, password, poolName)
    val dataSource = new HikariDataSource(config)
    // Closure: borrowed per execute(), returned on close. The
    // `getConnection()` blocks (briefly) if all connections are
    // in use; HikariCP's queue is bounded by connectionTimeout.
    () => {
      val conn = dataSource.getConnection()
      JdbcTrinoConnection.fromConnection(conn)
    }
  }
}
