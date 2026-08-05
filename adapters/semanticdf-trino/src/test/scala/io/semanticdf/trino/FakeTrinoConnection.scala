package io.semanticdf.trino

import io.semanticdf.core.expr.LiteralValue

/** A `FakeTrinoConnection` for testing `TrinoEngine.execute` without
  * a real Trino cluster.
  *
  * Holds:
  *   - canned `TrinoResult` per SQL string (key for `prepareStatement`)
  *   - a list of recorded `prepareStatement` calls (for assertions)
  *
  * Tests pre-populate the responses before calling the engine.
  * Top-level case class (not nested) so it doesn't capture the
  * enclosing test instance — necessary for clean test setup.
  */
final case class FakeTrinoConnection(
    responses: Map[(String, Int), TrinoResult] = Map.empty,
) extends TrinoConnection {

  /** Recorded calls — key is the SQL + number of parameters,
    * value is the number of times the SQL was executed. */
  val recordedCalls: scala.collection.mutable.Map[(String, Int), Int] =
    scala.collection.mutable.Map.empty

  override def prepareStatement(
      sql:        String,
      parameters: List[LiteralValue],
  ): TrinoResult = {
    val key = (sql, parameters.size)
    recordedCalls.update(key, recordedCalls.getOrElse(key, 0) + 1)
    responses.getOrElse(
      key,
      throw new RuntimeException(s"FakeTrinoConnection: no canned response for SQL: $sql (${parameters.size} params)"),
    )
  }

  override def close(): Unit = {
    // no-op for the fake — nothing to release
  }
}

object FakeTrinoConnection {

  /** A FakeTrinoConnection that returns the given result for the
    * given SQL + parameter count. */
  def withResponse(sql: String, parameters: Int, result: TrinoResult): FakeTrinoConnection =
    FakeTrinoConnection(
      responses = Map((sql, parameters) -> result),
    )
}