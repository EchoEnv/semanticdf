package io.semanticdf.duckdb

import io.semanticdf.core.expr.LiteralValue

/** Test fixture: a hand-driven [[DuckDBConnection]] that
  * returns scripted responses.
  *
  * Mirrors `FakeTrinoConnection` (per the existing
  * `TrinoEngine` test pattern). Tests register a
  * `sql-pattern -> DuckDBResult` lookup; unregistered lookups
  * return an empty result (matches DuckDB's "no rows" behavior
  * for missing data).
  *
  * ==Why a Map (not a behavior mock)==
  *
  * Per scala-data-driven-refacer §1: the fake is a data table
  * — it answers a deterministic question (does this SQL match a
  * registered response?) with a deterministic answer. No stubs,
  * no spies, no method-call recording. */
final class FakeDuckDBConnection(
    responses: Map[String, DuckDBResult],
    catchAll:  Option[DuckDBResult] = None,
) extends DuckDBConnection {

  /** The set of SQL strings this fake has been asked about. */
  val recordedCalls: scala.collection.mutable.ListBuffer[(String, Seq[Any])] =
    scala.collection.mutable.ListBuffer.empty

  override def prepareStatement(
      sql:        String,
      parameters: Seq[Any] = Nil,
  ): DuckDBResult = {
    recordedCalls += ((sql, parameters))
    responses.get(sql).orElse(catchAll).getOrElse(
      DuckDBResult(columns = List("__result"), rows = Nil),
    )
  }

  override def close(): Unit = ()
}

object FakeDuckDBConnection {

  /** Empty fake — every lookup returns an empty result. */
  val empty: FakeDuckDBConnection = new FakeDuckDBConnection(Map.empty)

  /** Build a fake with the given list of (sql-pattern, result) pairs. */
  def apply(responses: (String, DuckDBResult)*): FakeDuckDBConnection = {
    val m = responses.toMap
    new FakeDuckDBConnection(m)
  }

  /** Build a fake that returns the same result for every query. */
  def withCatchAll(result: DuckDBResult): FakeDuckDBConnection =
    new FakeDuckDBConnection(Map.empty, catchAll = Some(result))
}