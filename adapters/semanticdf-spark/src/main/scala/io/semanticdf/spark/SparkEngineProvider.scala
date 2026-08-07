package io.semanticdf.spark

import io.semanticdf.core.engine.{EngineContext, EngineError, EngineIdentity, MCPEngineProvider, MCPQueryRequest, PortableQueryResult, ResultRow, ResultSchema, ResultValue}
import io.semanticdf.core.expr.LiteralValue
import io.semanticdf.core.schema.{Field, SealedDataType}
import io.semanticdf.SemanticTable

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters._

/** `MCPEngineProvider` impl backed by an in-process Spark session.
  *
  * Translates the engine-portable `MCPQueryRequest` to a
  * `SemanticTable.query(...)` call, then collects + decodes
  * the rows into the portable `PortableQueryResult` shape.
  *
  * ==Why this lives in the spark adapter (not core)==
  *
  * Per scala-data-driven-refactor \u00a71: behavior in adapters.
  * The `SemanticTable` flow is Spark-specific; the
  * `DataFrame.collect()` + per-row `LiteralValue` decoding is
  * Spark-specific. The TRAIT (`MCPEngineProvider`) lives in
  * core; the IMPL lives here. */
final class SparkEngineProvider(
    spark: SparkSession,
    private val sparkTableRegistry: Map[String, SemanticTable],
) extends MCPEngineProvider {

  override val identity: EngineIdentity = EngineIdentity(
    name                 = "spark",
    nativeVersion        = spark.version,
    engineAdapterVersion = "0.2.4",
  )

  override val available: Boolean = spark != null && sparkTableRegistry.nonEmpty

  override def query(
      model:   io.semanticdf.core.model.Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    // The Spark provider currently uses the legacy
    // `SemanticTable` path (per the existing Query handler).
    // For v1, the MCP `Model` lookup is done by name in the
    // spark adapter's table registry. The `request.model` is
    // the name; we look it up in the spark registry.
    sparkTableRegistry.get(request.model) match {
      case None => Left(EngineError.ModelNotFound(request.model))
      case Some(table) => runQuery(table, request)
    }
  }

  override def explain(
      model:   io.semanticdf.core.model.Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, String] = {
    sparkTableRegistry.get(request.model) match {
      case None => Left(EngineError.ModelNotFound(request.model))
      case Some(table) => Right(table.query(
        measures   = request.measures,
        dimensions = request.dimensions,
        limit      = request.limit.map(_.toInt),
        timeGrain  = request.timeGrain,
        timeGrains = Map.empty,
        timeRange  = request.timeRange,
      ).explain())
    }
  }

  /** Run the Spark query, collect + decode the rows, and return
    * the portable `PortableQueryResult`. The per-row `LiteralValue`
    * decoding is the same as `TrinoResultEncoder` (engine-neutral
    * since both engines produce `LiteralValue` per their JDBC
    * type). */
  private def runQuery(
      table:   SemanticTable,
      request: MCPQueryRequest,
  ): Either[EngineError, PortableQueryResult] = {
    try {
      val st = table.query(
        measures   = request.measures,
        dimensions = request.dimensions,
        limit      = request.limit.map(_.toInt),
        timeGrain  = request.timeGrain,
        timeGrains = Map.empty,
        timeRange  = request.timeRange,
      )
      // execute(spark) -> DataFrame; collect() -> Array[Row]
      val df = st.execute(spark)
      val schema = ResultSchema(
        (table.dimensions.toSeq.map { case (name, _) => Field(name, SealedDataType.Varchar, nullable = true) } ++
          table.measures.toSeq.map { case (name, _) => Field(name, SealedDataType.Varchar, nullable = true) }).toList,
      )
      val rows: Vector[ResultRow] = df.collect().map { row =>
        ResultRow(
          values = row.toSeq
            .map(SparkEngineProvider.decodeCell)
            .map(SparkEngineProvider.toResultValue)
            .toList,
          schema = schema,
        )
      }.toVector
      Right(PortableQueryResult(
        schema   = schema,
        rows     = rows,
        metadata = Map("engine.adaptor.id" -> "spark", "engine.adaptor.version" -> "0.2.4"),
      ))
    } catch {
      case e: Exception => Left(EngineError.ConnectionFailed(
        reason = s"spark.query failed: ${e.getMessage}",
      ))
    }
  }
}

object SparkEngineProvider {
  // Decode a single Spark Row cell to a portable LiteralValue.
  // Same mapping as the Trino adapter's TrinoResultEncoder —
  // the LiteralValue shape is engine-neutral.
  private[spark] def decodeCell(v: Any): LiteralValue = v match {
    case null                => LiteralValue.NullValue
    case s: String           => LiteralValue.StringValue(s)
    case l: Long             => LiteralValue.LongValue(l)
    case i: Int              => LiteralValue.LongValue(i.toLong)
    case d: Double           => LiteralValue.DoubleValue(d)
    case b: Boolean          => LiteralValue.BoolValue(b)
    case bd: java.math.BigDecimal => LiteralValue.DecimalValue(bd)
    case other               => LiteralValue.StringValue(other.toString)
  }

  // Map a LiteralValue to a ResultValue (the portable shape from
  // PR #400). Per the v0.3.0 design review: values: List[Any] violates
  // the §1.3 transitively-serializable invariant. The sealed
  // ResultValue ADT forces every consumer to handle the closed
  // set of value shapes.
  private[spark] def toResultValue(lv: LiteralValue): ResultValue = lv match {
    case LiteralValue.NullValue              => ResultValue.NullV
    case LiteralValue.BoolValue(b)            => ResultValue.BoolV(b)
    case LiteralValue.IntValue(n)             => ResultValue.IntV(n.toLong)
    case LiteralValue.LongValue(n)            => ResultValue.IntV(n)
    case LiteralValue.FloatValue(f)           => ResultValue.DoubleV(f.toDouble)
    case LiteralValue.DoubleValue(d)          => ResultValue.DoubleV(d)
    case LiteralValue.DecimalValue(d)         => ResultValue.DecimalV(d)
    case LiteralValue.StringValue(s)          => ResultValue.StringV(s)
    case LiteralValue.TimestampValue(instant) => ResultValue.TimestampV(instant)
    case LiteralValue.DateValue(date)         => ResultValue.DateV(date)
    case other                               => ResultValue.StringV(other.toString)
  }
}