package io.semanticdf.duckdb

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

/** HikariCP-backed connection pool factory for DuckDB —
  * mirrors `TrinoConnectionPoolFactory` (PR #389).
  *
  * ==Why a pool==
  *
  * Per the user's "monitor memory, disk" constraint + the
  * standard production-throughput argument: opening a JDBC
  * connection has cost (driver init, schema load, session
  * metadata, etc.). For high-throughput query patterns, a
  * pool keeps N connections warm and serves queries without
  * the per-query init cost. For low-throughput or one-off
  * usage, the pool is unnecessary — `JdbcDuckDBConnection.fromUrl(url)`
  * is the simpler one-shot path.
  *
  * ==Why HikariCP (not DBCP / Vibur / etc.)==
  *
  * Same reasoning as `TrinoConnectionPoolFactory`: HikariCP is
  * the most-mature, fastest JDBC pool in the JVM ecosystem.
  * Per karpathy §2 ("don't add abstractions for single-use
  * code"): no need to evaluate alternatives until HikariCP
  * proves limiting.
  *
  * ==Why in-memory DuckDB works with HikariCP==
  *
  * DuckDB's embedded engine is **process-level** (one
  * connection = one engine instance). Pooling N connections
  * means N engines in the same JVM. For high-throughput this
  * is wasteful (N× memory); for multi-threaded workloads it
  * is the only correct option (DuckDB has limited cross-
  * connection concurrency). HikariCP manages this correctly:
  * the pool's `maximumPoolSize` caps the engine count.
  *
  * ==Why file-based DuckDB is the production-recommended mode==
  *
  * For in-process analytics on local files (CSV, Parquet),
  * `jdbc:duckdb:/path/to/file.db` persists the database to
  * disk. The pool then shares connections to that single file.
  * This is the typical "embedded analytics" deployment pattern
  * that DuckDB's design targets.
  *
  * ==Why the hard timeout / leak detection==
  *
  * Per the user's "monitor memory, disk first" constraint:
  * the pool's `leakDetectionThreshold` (60s) bounds the
  * worst-case scenario where a borrowed connection is never
  * returned. A leak after 60s is logged loudly (and would
  * indicate an engine bug; we'd want to know).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
object DuckDBConnectionPoolFactory {

  /** Build a HikariCP `HikariConfig` for DuckDB. Exposed for
    * tests (so the test can inspect the config without
    * triggering HikariCP's eager `checkFailFast` on
    * `HikariDataSource` construction).
    *
    * @param jdbcUrl       the DuckDB JDBC URL (e.g.
    *                      `jdbc:duckdb:` for in-memory,
    *                      `jdbc:duckdb:/tmp/x.db` for file-based)
    * @param maxPoolSize   max connections (caps DuckDB engine
    *                      count; default 10)
    * @param minIdle       min idle connections (default 2)
    * @param username      DuckDB doesn't authenticate; this is
    *                      kept for JDBC-compat (default None)
    * @param password      see `username` (default None)
    * @return a HikariConfig (NOT yet instantiated — the caller
    *         must call `new HikariDataSource(config)`) */
  def buildHikariConfig(
      jdbcUrl:     String,
      maxPoolSize: Int               = 10,
      minIdle:     Int               = 2,
      username:    Option[String]    = None,
      password:    Option[String]    = None,
  ): HikariConfig = {
    val config = new HikariConfig()
    config.setJdbcUrl(jdbcUrl)
    config.setMaximumPoolSize(maxPoolSize)
    config.setMinimumIdle(minIdle)
    config.setConnectionTestQuery("SELECT 1")  // light probe
    config.setPoolName("semanticdf-duckdb-pool")
    // 60s leak detection: a borrowed connection that isn't
    // returned within 60s is logged. Engine bugs that fail
    // to close would surface here (matches Trino adapter).
    config.setLeakDetectionThreshold(60_000L)
    username.foreach(config.setUsername)
    password.foreach(config.setPassword)
    config
  }

  /** Build a pooled `() => DuckDBConnection` from a HikariCP
    * data source. The returned function borrows a connection
    * from the pool per call and closes it (returning to the
    * pool) — the engine's `finally` block calls `.close()`,
    * which for pool-sourced connections is "return to pool",
    * not "terminate".
    *
    * @param jdbcUrl     the DuckDB JDBC URL
    * @param maxPoolSize max connections (default 10)
    * @return a function that, per call, borrows a connection
    *         from the pool and returns a `JdbcDuckDBConnection`
    *         wrapping it */
  def hikari(
      jdbcUrl:     String,
      maxPoolSize: Int = 10,
  ): () => DuckDBConnection = {
    val config  = buildHikariConfig(jdbcUrl, maxPoolSize = maxPoolSize)
    val source  = new HikariDataSource(config)
    () => {
      val conn = source.getConnection()
      JdbcDuckDBConnection.fromConnection(conn)
    }
  }
}