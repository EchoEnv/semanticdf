package io.semanticdf.trino.integration

import io.semanticdf.trino.{JdbcTrinoConnection, TrinoConnection, TrinoEngine}

/** Wires a `TrinoEngine` against a **real Trino cluster** via the
  * production `JdbcTrinoConnection` (PR #386). This file now is a
  * thin factory — the wiring details live in production code so
  * users can replicate it.
  *
  * ==Why this is test-only now==
  *
  * Per scala-data-driven-refactor §1: this is *behavior*, not data —
  * the engine factory setup is test/dev setup, not engine code.
  * Production users follow the same pattern (instantiate a
  * `TrinoEngine`, call `withConnectionFactory(() => new
  * JdbcTrinoConnection(url))`) but probably in their own bootstrap.
  *
  * ==Why a fresh engine per call==
  *
  * Per the standing pattern: tests are independent. Each call
  * produces a new engine — no shared mutable state, no leaked
  * connection. The cost is ~1ms per test. */
object TrinoIntegrationSupport {

  /** Build a `TrinoEngine` wired to a real Trino cluster at
    * `trinoUrl`. Each `execute()` opens a fresh JDBC connection
    * (closed via `finally`). */
  def engineWithConnection(trinoUrl: String): TrinoEngine = {
    val connectionFactory: () => TrinoConnection = () =>
      JdbcTrinoConnection.fromUrl(trinoUrl)
    new TrinoEngine().withConnectionFactory(connectionFactory)
  }
}
