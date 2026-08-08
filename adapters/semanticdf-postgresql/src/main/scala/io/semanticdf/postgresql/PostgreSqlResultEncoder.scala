package io.semanticdf.postgresql

import io.semanticdf.core.engine.{PortableQueryResult, ResultRow, ResultSchema, ResultValue}
import io.semanticdf.core.schema.{Field, SealedDataType}

/** Minimal v1 [[io.semanticdf.core.engine.ResultEncoder]] for
  * PostgreSQL query results.
  *
  * Mirrors `io.semanticdf.hera.HeraResultEncoder` (PR #425) in shape.
  *
  * ==Why a minimal v1 encoder==
  *
  * Per karpathy §2 ("minimum code that solves the problem"): the
  * `Engine.executePortable` default throws `NotImplementedError`,
  * which is deprecated per `docs/design/error-handling-style.md`.
  * We need a real impl.
  *
  * For v1 we carry the columns as `name -> SealedDataType` and the
  * rows as `List[ResultValue]`. A richer v0.4.0 encoder can use
  * PG's `pg_typeof()` per column to map to the exact portable type. */
object PostgreSqlResultEncoder {

  /** Encode a [[PostgreSqlResult]] into a portable
    * [[PortableQueryResult]]. */
  def encode(result: PostgreSqlResult): PortableQueryResult = {
    val fields = result.columns.map { c =>
      Field(name = c.name, dataType = typeToSealed(c.dataType), nullable = c.nullable)
    }
    val schema = ResultSchema(fields = fields)
    val rows = result.rows.map { row =>
      val values = result.columns.map { c =>
        toResultValue(row.getOrElse(c.name, null))
      }
      ResultRow(values = values, schema = schema)
    }
    PortableQueryResult(
      schema   = schema,
      rows     = rows.toVector,
      metadata = Map.empty,
    )
  }

  /** Map a PostgreSQL type-name string to a portable [[SealedDataType]].
    *
    * Mirrors the type-mapper in the existing UC/HMS/Hera adapters
    * (different naming per engine, same set of cases). */
  private def typeToSealed(pgType: String): SealedDataType = pgType.toLowerCase match {
    case t if t.startsWith("bigint") || t.startsWith("int8")  => SealedDataType.BigInt
    case t if t.startsWith("int") || t.startsWith("int4")     => SealedDataType.Int
    case t if t.startsWith("smallint") || t.startsWith("int2") => SealedDataType.Int
    case t if t.startsWith("double") || t.startsWith("float8") => SealedDataType.Double
    case t if t.startsWith("real") || t.startsWith("float4")    => SealedDataType.Double
    case t if t.startsWith("numeric") || t.startsWith("decimal") => SealedDataType.Decimal(38, 10)
    case t if t.startsWith("boolean") || t.startsWith("bool")  => SealedDataType.Boolean
    case t if t.startsWith("text") || t.startsWith("varchar") || t.startsWith("char") => SealedDataType.Varchar
    case t if t.startsWith("timestamp") || t.startsWith("date") => SealedDataType.Timestamp
    case _ => SealedDataType.Varchar
  }

  /** Convert a raw value (from the JDBC ResultSet) into a
    * [[ResultValue]]. Best-effort for v1. */
  private def toResultValue(v: Any): ResultValue = v match {
    case null       => ResultValue.NullV
    case n: Long    => ResultValue.IntV(n)
    case n: Int     => ResultValue.IntV(n.toLong)
    case n: java.math.BigDecimal => ResultValue.DecimalV(BigDecimal(n))
    case n: Double  => ResultValue.DoubleV(n)
    case n: Float   => ResultValue.DoubleV(n.toDouble)
    case n: Boolean => ResultValue.BoolV(n)
    case n: java.sql.Date => ResultValue.DateV(n.toLocalDate)
    case n: java.sql.Timestamp => ResultValue.TimestampV(n.toInstant)
    case n: java.time.LocalDate => ResultValue.DateV(n)
    case n: java.time.Instant => ResultValue.TimestampV(n)
    case s: String  => ResultValue.StringV(s)
    case other      => ResultValue.StringV(other.toString)
  }
}