package io.semanticdf.hera

import io.semanticdf.core.engine.{
  PortableQueryResult,
  ResultRow,
  ResultSchema,
  ResultValue,
}
import io.semanticdf.core.schema.{Field, SealedDataType}

/** v0.3.1 (Hera adapter): minimal v1 ResultEncoder for Hera query
  * results.
  *
  * ==Why a minimal v1 encoder==
  *
  * Per karpathy §2 ("minimum code that solves the problem"): the
  * Engine trait's `executePortable` default throws
  * `NotImplementedError`, which is deprecated per
  * `docs/design/error-handling-style.md`. We need a real impl.
  *
  * For v1 we carry the columns as `name -> SealedDataType` (from
  * the field metadata) and the rows as `List[ResultValue]`
  * (NaN/null/string/etc. mapped via a best-effort type guess).
  * A richer v0.4.0 encoder can use Hera's `dataType` field per
  * column to map to the exact portable type.
  *
  * ==Why this lives here (not in `core`)==
  *
  * Per scala-data-driven-refacer §1: the SHAPE of the
  * `PortableQueryResult` ADT lives in core; the BODY (Hera-specific
  * encoding logic) lives here. */
object HeraResultEncoder {

  /** Encode a [[HeraQueryResult]] into a portable
    * [[PortableQueryResult]]. */
  def encode(result: HeraQueryResult): PortableQueryResult = {
    val fields = result.fields.map { f =>
      Field(name = f.name, dataType = heraTypeToSealed(f.dataType), nullable = f.nullable)
    }
    val schema = ResultSchema(fields = fields)
    val rows = result.rows.map { row =>
      val values = result.fields.map { f =>
        toResultValue(row.getOrElse(f.name, null))
      }
      ResultRow(values = values, schema = schema)
    }
    PortableQueryResult(
      schema   = schema,
      rows     = rows.toVector,
      metadata = Map.empty,
    )
  }

  /** Map a Hera type-name string to a portable [[SealedDataType]].
    * Mirrors the canonical mapping in HttpHeraClient (which is
    * private to that class — duplicated here to keep this encoder
    * standalone). */
  private def heraTypeToSealed(heraType: String): SealedDataType = heraType.toLowerCase match {
    case t if t.startsWith("bigint") || t.startsWith("long")   => SealedDataType.BigInt
    case t if t.startsWith("int")                              => SealedDataType.Int
    case t if t.startsWith("double")                           => SealedDataType.Double
    case t if t.startsWith("boolean") || t.startsWith("bool")  => SealedDataType.Boolean
    case t if t.startsWith("timestamp") || t.startsWith("date") => SealedDataType.Timestamp
    case _                                                    => SealedDataType.Varchar
  }

  /** Convert a raw value (from the JSON-parsed row) into a
    * [[ResultValue]]. Best-effort for v1 — we don't have a
    * Hera-specific type per row entry, so we guess from the value. */
  private def toResultValue(v: Any): ResultValue = v match {
    case null       => ResultValue.NullV
    case n: Long    => ResultValue.IntV(n)
    case n: Int     => ResultValue.IntV(n.toLong)
    case n: Double  => ResultValue.DoubleV(n)
    case n: Boolean => ResultValue.BoolV(n)
    case s: String  => ResultValue.StringV(s)
    case other      => ResultValue.StringV(other.toString)
  }
}