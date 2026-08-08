package io.semanticdf.myplatform

import io.semanticdf.core.engine.{PortableQueryResult, ResultRow, ResultSchema, ResultValue}
import io.semanticdf.core.schema.{Field, SealedDataType}

/** Minimal v1 [[io.semanticdf.core.engine.ResultEncoder]] for MyPlatform
  * query results.
  *
  * Mirrors `io.semanticdf.hera.HeraResultEncoder` (PR #425).
  *
  * ==Why a minimal v1 encoder==
  *
  * Per karpathy §2 ("minimum code that solves the problem"): the
  * `Engine.executePortable` default throws `NotImplementedError`, which
  * is deprecated per `docs/design/error-handling-style.md`. We need
  * a real impl.
  *
  * For v1 we carry the columns as `name -> SealedDataType` and the
  * rows as `List[ResultValue]`. A richer v0.4.0 encoder can use
  * MyPlatform's `dataType` field per column to map to the exact
  * portable type. */
object MyPlatformResultEncoder {

  /** Encode a [[MyPlatformResult]] into a portable
    * [[PortableQueryResult]]. */
  def encode(result: MyPlatformResult): PortableQueryResult = {
    val fields = result.fields.map { f =>
      Field(name = f.name, dataType = typeToSealed(f.dataType), nullable = f.nullable)
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

  /** Map a MyPlatform type-name string to a portable [[SealedDataType]]. */
  private def typeToSealed(myPlatformType: String): SealedDataType = myPlatformType.toLowerCase match {
    case t if t.startsWith("bigint") || t.startsWith("long")   => SealedDataType.BigInt
    case t if t.startsWith("int")                              => SealedDataType.Int
    case t if t.startsWith("double")                           => SealedDataType.Double
    case t if t.startsWith("boolean") || t.startsWith("bool")  => SealedDataType.Boolean
    case t if t.startsWith("timestamp") || t.startsWith("date") => SealedDataType.Timestamp
    case _                                                    => SealedDataType.Varchar
  }

  /** Convert a raw value (from the JSON-parsed row) into a
    * [[ResultValue]]. Best-effort for v1. */
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