package io.semanticdf.trino

import io.semanticdf.core.engine.{PortableQueryResult, ResultError, ResultEncoder, ResultRow, ResultSchema, ResultValue}
import io.semanticdf.core.expr.LiteralValue
import io.semanticdf.core.schema.{Field, SealedDataType}

import java.sql.{ResultSet, ResultSetMetaData}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

/** `ResultEncoder[TrinoResult]` \u2014 translates a Trino
  * `TrinoResult` (columns + rows) into the engine-portable
  * `PortableQueryResult` (typed schema + typed values).
  *
  * ==Why a class (not an `object`)==
  *
  * Per scala-data-driven-refactor \u00a71: data in core, behavior
  * in adapters. The encoder is **behavior** \u2014 the Trino JDBC
  * ResultSet-to-typed-value mapping is engine-specific. The
  * class lives in the Trino adapter; the trait
  * (`ResultEncoder[-R]`) lives in core.
  *
  * ==Why `sealed trait` returns (per the design)==
  *
  * The encoder either succeeds (returns a `PortableQueryResult`)
  * or fails with a typed `ResultError`. Per the design \u00a74.5.4:
  * a failed encoding is a typed error, not a thrown exception.
  *
  * ==Per the design \u00a74.5.4 conformance properties==
  *
  * 1. `null` is JVM null and rejected in non-null fields.
  * 2. Decimals preserve declared precision and scale.
  * 3. Timestamps normalize to UTC `Instant`.
  * 4. Dates are `LocalDate`.
  *
  * The encoder's `LiteralValue \u2192 ResultValue` mapping is the
  * single point of truth for these properties. */
final class TrinoResultEncoder extends ResultEncoder[TrinoResult] {

  override def encode(r: TrinoResult): Either[ResultError, PortableQueryResult] = {
    // Build the typed schema from the columns + an inferred
    // `SealedDataType` for each. v1: we use the column-name-
    // only schema (Trino's `TrinoResult` carries `columns:
    // List[String]`, not `List[Field]`). Future PRs can wire
    // a richer schema.
    val schema = ResultSchema(r.columns.map { name =>
      Field(name = name, dataType = SealedDataType.Varchar, nullable = true)
    })

    // Map each `LiteralValue` row to a `ResultValue` row.
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
        metadata = Map("engine.adaptor.id" -> "trino", "engine.adaptor.version" -> "0.3.0"),
      )
    }
  }

  /** Map a `LiteralValue` to a `ResultValue` (the typed
    * value-shapes). The mapping preserves the design's
    * conformance properties: decimals keep precision/scale,
    * timestamps normalize to UTC `Instant`, dates are
    * `LocalDate`. */
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
    // For unhandled / future cases (BinaryValue, ArrayValue, etc.),
    // fall through to a string. Per the design: "arrays are
    // recursive Vector[Any]; structs are nested ResultRow;
    // maps are ordered Vector[(Any,Any)]" \u2014 future PRs add
    // the proper shapes.
    case other => ResultValue.StringV(other.toString)
  }
}