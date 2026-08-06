package io.semanticdf.duckdb

/** Engine-internal DuckDB connection boundary trait.
  *
  * Mirrors the `TrinoConnection` trait pattern (in
  * `adapters/semanticdf-trino`) — the `DuckDBEngine` depends on
  * this trait (not on a concrete JDBC connection), so tests can
  * inject a fake implementation without spinning up a real
  * DuckDB instance.
  *
  * ==Why a trait (vs. a concrete JDBC connection)==
  *
  * Per scala-data-driven-refacer §1 ("data is data, behavior
  * lives elsewhere"): the BEHAVIOR (calling DuckDB, parsing
  * results) is engine-specific. The CONTRACT (the methods the
  * engine needs) is in this trait — it's a small abstraction
  * that's justified by testability needs.
  *
  * ==Why core has no `DuckDBConnection`==
  *
  * `DuckDBConnection` is engine-specific (only DuckDB uses it).
  * It lives in the DuckDB adapter, NOT in core. The core
  * contract is the `Engine` trait; each engine adapter provides
  * its own connection boundary.
  *
  * ==Why `prepareStatement` returns a `DuckDBResult` (not `Any`)==
  *
  * The `Engine[R]` trait uses `R = Any` for the DuckDB adapter
  * (mirroring the Trino adapter). The `prepareStatement` method
  * here returns a typed `DuckDBResult` directly so the engine
  * can pattern-match on it without casting. This is the same
  * pattern as `TrinoConnection.prepareStatement` returning a
  * `TrinoResult`.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-duckdb/src/main/scala/io/semanticdf/duckdb/DuckDBConnection.scala`
  */
trait DuckDBConnection extends Serializable {

  /** Prepare + execute a parameterized statement. Returns the
    * result rows + columns. Throws on connection failure or
    * query error (the engine maps exceptions to EngineError).
    *
    * The connection is closed by the caller in `finally` (per
    * the engine's contract). For pooled connections, the pool's
    * close() returns the connection to the pool rather than
    * terminating it.
    *
    * @param sql         the parameterized SQL with `?` placeholders
    * @param parameters  the bind values (positions match `?`)
    * @return the result (columns + rows) */
  def prepareStatement(
      sql:        String,
      parameters: Seq[Any] = Nil,
  ): DuckDBResult

  /** Close the connection. Idempotent. After close(), any
    * further prepareStatement call throws. */
  def close(): Unit
}