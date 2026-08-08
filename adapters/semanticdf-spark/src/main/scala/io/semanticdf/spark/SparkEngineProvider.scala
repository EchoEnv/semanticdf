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
    engineAdapterVersion = "0.3.0",
  )

  override val available: Boolean = spark != null && sparkTableRegistry.nonEmpty

  override def query(
      model:   io.semanticdf.core.model.Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    // v0.3.1 (Gap 1 closure): the Spark engine adapter now
    // routes portable `Model`s through `PortableQueryCompiler`
    // instead of the legacy `SemanticTable` fluent chain.
    // The legacy path is retained for the `explain` case
    // (and for any Model that the user opted NOT to build
    // via `Model.of(...)`).
    //
    // The portable path picks up dimensions, measures,
    // filters, and joins from the Model itself. The
    // MCP request's `measures` / `dimensions` / `limit` are
    // applied as request-level overrides AFTER compile
    // (limit) or honored if they match the Model's shape.
    //
    // Per JVM-safety §3: the SparkSession is set on the
    // global carrier for the duration of this call, then
    // cleared. No instance state is captured into the
    // Spark closure (per scala-spark-batch-bugs §1: closures
    // are stateless Column expressions only).
    PortableQueryCompiler.setSparkSession(spark)
    try {
      runPortableQuery(model, request, ctx)
    } finally {
      PortableQueryCompiler.clearSparkSession()
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
        metadata = Map("engine.adaptor.id" -> "spark", "engine.adaptor.version" -> "0.3.0"),
      ))
    } catch {
      case e: Exception => Left(EngineError.ConnectionFailed(
        reason = s"spark.query failed: ${e.getMessage}",
      ))
    }
  }

  /** Run the portable path: compile the portable `Model` via
    * `PortableQueryCompiler`, apply per-request overrides
    * (limit), collect rows, decode to portable values, return
    * `PortableQueryResult`.
    *
    * Per scala-spark-batch-bugs §1: closures captured into
    * the Spark plan are stateless Column expressions only
    * (no UDFs, no captured variables from this method's
    * scope). No `NotSerializableException` hazard.
    *
    * Per scala-spark-batch-bugs §3 (schema drift): schema
    * assumptions are verified at the boundary — the
    * DataFrame's `schema: StructType` is the source of
    * truth for the result schema. We don't trust the
    * caller-supplied model dimensions/measures for the
    * output column types; we read them from the actual
    * compiled plan. */
  private def runPortableQuery(
      model:   io.semanticdf.core.model.Model,
      request: MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    try {
      new PortableQueryCompiler().compile(model, ctx).flatMap { df =>
        // Apply per-request limit (the only request-level
        // override for v0.3.1; orderBy support deferred).
        val limited = request.limit.fold(df)(l => df.limit(l.toInt))

        // Derive the result schema from the compiled plan's
        // actual schema (per scala-spark-batch-bugs §3:
        // verify, don't assume).
        val sparkSchema = limited.schema
        val schema = ResultSchema(sparkSchema.fields.map { f =>
          Field(
            name     = f.name,
            dataType = sparkTypeToSealedDataType(f.dataType),
            nullable = f.nullable,
          )
        }.toList)

        val rows: Vector[ResultRow] = limited.collect().map { row =>
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
          metadata = Map("engine.adaptor.id" -> "spark", "engine.adaptor.version" -> "0.3.0"),
        ))
      }
    } catch {
      case e: Exception =>
        // Per `docs/design/error-handling-style.md` ("catch-all cleanup"
        // section): distinguish query runtime errors from connection
        // failures. Spark's DataFrame operations throw
        // `AnalysisException` (and similar) for query runtime issues
        // — those map to `QueryRuntimeFailed`. Anything else
        // (network, classloading, etc.) maps to `ConnectionFailed`.
        // This is the "start small" piece; legacy `runQuery` still
        // uses the coarse catch-all (tracked for follow-up).
        e match {
          case _: org.apache.spark.sql.AnalysisException =>
            Left(EngineError.QueryRuntimeFailed(
              reason = s"spark.portable-query analysis failed: ${e.getClass.getSimpleName}: ${e.getMessage}",
            ))
          case _ =>
            Left(EngineError.ConnectionFailed(
              reason = s"spark.portable-query failed: ${e.getClass.getSimpleName}: ${e.getMessage}",
            ))
        }
    }
  }

  /** Map a Spark `DataType` to the portable `SealedDataType`.
    * Per scala-spark-batch-bugs §3: never assume; verify at
    * the boundary. Unsupported Spark types fall back to
    * `SealedDataType.Json` so the row data still serializes
    * (the actual value is a JSON string). */
  private[spark] def sparkTypeToSealedDataType(
      dt: org.apache.spark.sql.types.DataType,
  ): SealedDataType = dt match {
    case org.apache.spark.sql.types.StringType       => SealedDataType.Varchar
    case org.apache.spark.sql.types.LongType         => SealedDataType.Int   // portable has no Long; widen semantics
    case org.apache.spark.sql.types.IntegerType      => SealedDataType.Int
    case org.apache.spark.sql.types.DoubleType       => SealedDataType.Double
    case org.apache.spark.sql.types.FloatType        => SealedDataType.Double
    case org.apache.spark.sql.types.BooleanType      => SealedDataType.Boolean
    case org.apache.spark.sql.types.TimestampType     => SealedDataType.Timestamp
    case org.apache.spark.sql.types.DateType         => SealedDataType.Date
    case _: org.apache.spark.sql.types.DecimalType    => SealedDataType.Decimal(38, 18)  // Spark default precision/scale
    case _: org.apache.spark.sql.types.ArrayType      => SealedDataType.Json  // array JSON-encoded
    case _: org.apache.spark.sql.types.MapType        => SealedDataType.Json
    case _: org.apache.spark.sql.types.StructType     => SealedDataType.Json
    case _: org.apache.spark.sql.types.BinaryType     => SealedDataType.Json
    case _                                            => SealedDataType.Json  // unknown → JSON fallback
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