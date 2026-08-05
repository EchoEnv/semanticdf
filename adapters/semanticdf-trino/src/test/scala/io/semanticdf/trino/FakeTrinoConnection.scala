package io.semanticdf.trino

import io.semanticdf.core.expr.LiteralValue

/** A `FakeTrinoConnection` for testing `TrinoEngine.execute` without
  * a real Trino cluster.
  *
  * Holds:
  *   - canned `TrinoResult` per SQL string (key for `prepareStatement`)
  *     — when SQL + parameter count matches, this exact response
  *     is returned.
  *   - optional `catchAll` result — when set, any unmatched SQL
  *     returns this response (handy when tests care about the
  *     engine's behaviour but don't want to enumerate exact SQL
  *     shapes built dynamically by the compiler).
  *   - recorded `prepareStatement` calls (for assertions)
  *
  * Tests pre-populate the responses before calling the engine.
  * Top-level case class (not nested) so it doesn't capture the
  * enclosing test instance — necessary for clean test setup.
  *
  * ==Why both `responses` and `catchAll` (not just one)==
  *
  * Per scala-data-driven-refactor §3 ("rule becomes data only
  * when it must change without a deploy"):
  *   - `responses`: SQL-keyed lookup, used by tests that assert
  *     on specific statements (e.g. "SQL contained the right
  *     GROUP BY clause").
  *   - `catchAll`: blanket fallback, used by tests that don't
  *     care about exact SQL — they assert on engine behaviour
  *     (execute → rows returned) without enumerating the SQL
  *     shape.
  *
  * Both are needed; neither subsumes the other.
  *
  * ==Why `catchAll` lives on the connection (vs. a separate helper)==
  *
  * Per karpathy §3 (surgical changes): it's the same data shape as
  * `responses`, just keyed differently. Adding the field is one
  * line + a `getOrElse` fallback in `prepareStatement`. A separate
  * helper class would multiply types without simplifying tests. */
final case class FakeTrinoConnection(
    responses: Map[(String, Int), TrinoResult] = Map.empty,
    catchAll:  Option[TrinoResult]             = None,
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
    responses.get(key).orElse(catchAll).getOrElse {
      throw new RuntimeException(
        s"FakeTrinoConnection: no canned response for SQL: $sql (${parameters.size} params)",
      )
    }
  }

  override def close(): Unit = {
    // no-op for the fake — nothing to release
  }
}

object FakeTrinoConnection {

  /** A FakeTrinoConnection that returns the given result for the
    * given SQL + parameter count. */
  def withResponse(
      sql:        String,
      parameters: Int,
      result:     TrinoResult,
  ): FakeTrinoConnection =
    FakeTrinoConnection(
      responses = Map((sql, parameters) -> result),
    )

  /** A FakeTrinoConnection that returns the given result for
    * ANY `prepareStatement` call (regardless of SQL or parameter
    * count). Useful when the test cares about engine behaviour
    * (rows returned, errors) but doesn't want to assert on exact
    * SQL shape.
    *
    * Per scala-data-driven-refactor §3: the catch-all is a
    * "fallback rule" — its presence here doesn't upgrade the
    * `responses` map to a table; the two are complementary data
    * shapes (specific lookup + blanket fallback).
    *
    * ==Why this factory exists==
    *
    * Tests that build the expected SQL dynamically (via
    * `engine.compile(m, ctx).toOption.get.native.asInstanceOf[
    * ParameterizedSql].sql`) would otherwise need to repeat the
    * compile + LIMIT-n concatenation in the test. `catchAll`
    * avoids that boilerplate when the test doesn't care about
    * the SQL exact shape. */
  def catchAll(result: TrinoResult): FakeTrinoConnection =
    FakeTrinoConnection(catchAll = Some(result))
}
