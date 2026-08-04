package io.semanticdf.trino

import io.semanticdf.core.schema.{Field, SealedDataType}

/** Engine-specific Trino-client boundary trait.
  *
  * This trait is the boundary between the engine-portable
  * `SourceResolver` contract (in core) and the engine-specific
  * Trino implementation. The `TrinoSourceResolver` depends on
  * this trait (not on a concrete Trino JDBC connection), so
  * tests can inject a fake implementation.
  *
  * ==Why a trait (vs. a concrete Trino JDBC connection)==
  *
  * Per scala-data-driven-refactor §1 ("data is data, behavior
  * lives elsewhere"): the BEHAVIOR (calling Trino, parsing
  * responses) is engine-specific. The CONTRACT (the methods
  * the resolver needs) is in this trait — it's a small
  * abstraction that's justified by testability needs.
  *
  * ==Why core has no `TrinoClient`==
  *
  * `TrinoClient` is engine-specific (only Trino uses it). It
  * lives in the Trino adapter, NOT in core. The core contract
  * is `SourceResolver` (engine-portable); each engine adapter
  * provides its own resolver AND its own client boundary.
  *
  * ==Why each method returns either `Option` or a typed result==
  *
  * `describeTable` returns `Option[TrinoTableSchema]` because
  * the table might not exist. `getTableStats` returns
  * `Option[Long]` because Trino might not be able to compute
  * stats for the table (e.g. for views).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports (this is the Trino adapter — no Spark
  * dependencies). Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/`
  */
trait TrinoClient extends Serializable {

  /** Describe a table: return its columns (name + type) in the
    * order Trino reports them.
    *
    * Returns `None` if the table doesn't exist or the user
    * doesn't have permission to describe it (auth failure is
    * mapped to `ResolvedSource.AuthFailed` by the caller).
    *
    * @param catalog the catalog name (e.g. "hive", "iceberg")
    * @param schema  the schema / namespace name (e.g. "public",
    *                "silver")
    * @param table   the table name */
  def describeTable(
      catalog: String,
      schema:  String,
      table:   String,
  ): Option[TrinoTableSchema]

  /** Estimate the row count of a table (via `SHOW STATS FOR`).
    *
    * Returns `None` if stats are unavailable (views, unanalyzed
    * tables, etc.). Some implementations may approximate via
    * `SELECT COUNT(*)`; the caller decides.
    *
    * @param catalog the catalog name
    * @param schema  the schema name
    * @param table   the table name */
  def getTableRowCount(
      catalog: String,
      schema:  String,
      table:   String,
  ): Option[Long]
}

/** A `TrinoClient.describeTable` result — the table's columns
  * (name + portable `SealedDataType`) in declaration order.
  *
  * Per scala-data-driven-refactor §1: this is pure data — no
  * behavior, no closures. The columns list is immutable
  * (`List[Field]`). The mapping from Trino's native types
  * (`bigint`, `varchar`, etc.) to portable `SealedDataType` is
  * the engine adapter's job; this case class carries the
  * already-mapped result.
  */
final case class TrinoTableSchema(
    columns: List[Field],
) extends Product with Serializable {

  /** Convenience: look up a column by name. Returns `None` if
    * the column doesn't exist (case-sensitive). */
  def column(name: String): Option[Field] = columns.find(_.name == name)

  /** Convenience: does the table have a column with this name? */
  def hasColumn(name: String): Boolean = column(name).isDefined
}

/** Companion factory: build a `TrinoTableSchema` from a list of
  * (name, dataType) pairs. The mapper from Trino's native type
  * strings to `SealedDataType` is the caller's job (in
  * `TrinoSourceResolver`). */
object TrinoTableSchema {

  /** Build a `TrinoTableSchema` from a list of (name, type) pairs.
    * Convenience for test fixtures and for the
    * `TrinoSourceResolver` mapper. */
  def of(pairs: List[(String, SealedDataType)]): TrinoTableSchema =
    TrinoTableSchema(
      columns = pairs.map { case (name, dataType) =>
        Field(name = name, dataType = dataType, nullable = true)
      },
    )
}