package io.semanticdf.duckdb

import io.semanticdf.core.expr.LiteralValue

/** Engine-specific DuckDB query result shape.
  *
  * Mirrors `TrinoResult` (in `adapters/semanticdf-trino`) — the
  * columns + rows + helpers used by the engine's terminal
  * operations (`executeAsRows`, `previewAsRows`, `count`,
  * `toJson`, `isEmpty`).
  *
  * ==Why `List[List[LiteralValue]]` for rows==
  *
  * Per scala-data-driven-refacer §1: rows are pure data — a list
  * of lists, where each inner list represents one row's column
  * values. Each value is a `LiteralValue` (engine-portable
  * primitive) so the consumer can pattern-match on it without
  * caring which engine produced it.
  *
  * ==Why `extends Product with Serializable`==
  *
  * `Product` = auto-generated `equals`/`hashCode`/`toString`
  * (handy for test assertions). `Serializable` = Java-serialization
  * round-trip safety. Case classes get these for free; we
  * explicitly mark them here for clarity.
  *
  * ==Why `rowCount` + `isEmpty` + `cell` as methods (not fields)==
  *
  * They are pure derivations from the `rows` list. Per the
  * data-driven mantra, simple accessors belong on the data
  * class; per scala-data-driven-refacer §1, "a method belongs
  * on the data type only if it's cheap, total, pure, and purely
  * a function of the fields already there" — these qualify.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final case class DuckDBResult(
    columns: List[String],
    rows:    List[List[LiteralValue]],
) extends Product with Serializable {

  /** Number of rows in the result. Mirrors `TrinoResult.rowCount`
    * for the cross-engine comparison story. */
  def rowCount: Int = rows.size

  /** True iff no rows are present. */
  def isEmpty: Boolean = rows.isEmpty

  /** Cell value at (rowIdx, colIdx). Returns `None` for out-of-
    * bounds indices or null values. */
  def cell(rowIdx: Int, colIdx: Int): Option[LiteralValue] =
    rows.lift(rowIdx).flatMap(_.lift(colIdx))
}