package io.semanticdf.duckdb

import io.semanticdf.core.engine.{PortableQueryResult, ResultError, ResultEncoder, ResultRow, ResultSchema, ResultValue}
import io.semanticdf.core.expr.LiteralValue
import io.semanticdf.core.schema.{Field, SealedDataType}

/** `ResultEncoder[DuckDBResult]` \u2014 mirrors
  * `TrinoResultEncoder`. Translates a DuckDB `DuckDBResult`
  * into the engine-portable `PortableQueryResult`.
  *
  * ==Why a separate class (not a shared trait impl)==
  *
  * Per scala-data-driven-refactor \u00a71: data in core, behavior
  * in adapters. The DuckDB-specific ResultSet-to-typed-value
  * mapping is **engine-specific** \u2014 it lives in the DuckDB
  * adapter, not in core.
  *
  * ==Why a copy of the `toResultValue` helper (not a shared
  * base class)==
  *
  * The `LiteralValue \u2192 ResultValue` mapping is engine-
  * neutral (every engine produces the same `LiteralValue` per
  * its own JDBC type). The mapping COULD live in core as a
  * shared helper \u2014 but per karpathy \u00a72 (minimum code that
  * solves the problem), the duplicated 5-line helper is
  * cheaper than the abstraction. If a third engine joins and
  * the duplication becomes painful, the shared helper is a
  * 1-line follow-up PR. */
final class DuckDBResultEncoder extends ResultEncoder[DuckDBResult] {

  override def encode(r: DuckDBResult): Either[ResultError, PortableQueryResult] = {
    val schema = ResultSchema(r.columns.map { name =>
      Field(name = name, dataType = SealedDataType.Varchar, nullable = true)
    })

    val rowsEither: Either[ResultError, Vector[ResultRow]] = r.rows.zipWithIndex.foldLeft(
      Right(Vector.empty[ResultRow]): Either[ResultError, Vector[ResultRow]]
    ) { case (acc, (row, idx)) =>
      acc.flatMap { accRows =>
        if (row.size != schema.fields.size) {
          Left(ResultError.ShapeMismatch(
            reason = s"row $idx has ${row.size} values but schema has ${schema.fields.size} fields",
          ))
        } else {
          val values = row.map(toResultValue(_, idx))
          Right(accRows :+ ResultRow(values, schema))
        }
      }
    }

    rowsEither.map { rows =>
      PortableQueryResult(
        schema   = schema,
        rows     = rows,
        metadata = Map("engine.adaptor.id" -> "duckdb", "engine.adaptor.version" -> "0.2.4"),
      )
    }
  }

  /** Same mapping as `TrinoResultEncoder` \u2014 the `LiteralValue`
    * shape is engine-neutral. Per the note above, we duplicate
    * here per karpathy \u00a72 (minimum code) and refactor when
    * a third engine arrives. */
  private def toResultValue(
      lv:    LiteralValue,
      rowIdx: Int,
  ): ResultValue = lv match {
    case LiteralValue.NullValue       => ResultValue.NullV
    case LiteralValue.BoolValue(b)    => ResultValue.BoolV(b)
    case LiteralValue.IntValue(n)     => ResultValue.IntV(n.toLong)
    case LiteralValue.LongValue(n)    => ResultValue.IntV(n)
    case LiteralValue.FloatValue(f)   => ResultValue.DoubleV(f.toDouble)
    case LiteralValue.DoubleValue(d)  => ResultValue.DoubleV(d)
    case LiteralValue.DecimalValue(d) => ResultValue.DecimalV(d)
    case LiteralValue.StringValue(s)  => ResultValue.StringV(s)
    case LiteralValue.TimestampValue(instant) => ResultValue.TimestampV(instant)
    case LiteralValue.DateValue(date) => ResultValue.DateV(date)
    case other => ResultValue.StringV(other.toString)
  }
}