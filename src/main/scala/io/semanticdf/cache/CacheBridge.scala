package io.semanticdf.cache

import io.semanticdf.SemanticTable
import io.semanticdf.audit.QueryRequest
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.jdk.CollectionConverters._

/** Java-callable facade for the library's compile + execute + cache
  * path. The platform's `QueryService` calls these so we don't fight
  * Scala-2.13's Iterable / Seq / Option generic plumbing through Java.
  *
  * This file is in the library (not the platform) because:
  *   1. It compiles inside the existing scala-maven-plugin invocation.
  *   2. It's a thin wrapper around library primitives \u2014 nothing
  *      platform-specific.
  *
  * All methods are pure (deterministic) for a given model + request +
  * spark session; the platform wraps the JVM-side effects in
  * `Restate.run(...)` for replay safety.
  */
object CacheBridge {

  /** Run a model query against the given spark session, returning a
    * {@link CachedResult} that wraps the materialized {@code Row[]} and
    * the result {@link org.apache.spark.sql.types.StructType}.
    *
    * @param model              the compiled semantic model
    * @param spark              active SparkSession
    * @param measures           column names \u2014 aggregated measures
    * @param dimensions         column names \u2014 group-by dimensions
    * @param where              optional Spark SQL filter expression
    *                           (passed to {@code df.filter} verbatim;
    *                           semanticddf structured Predicates land
    *                           in a v0.2.3 follow-up).
    */
  def executeQuery(
      model: SemanticTable,
      spark: SparkSession,
      measures: java.util.List[String],
      dimensions: java.util.List[String],
      where: String,
  ): CachedResult = {
    val df: DataFrame =
      model.query(measures.asScala, dimensions.asScala).toDataFrame(spark)
    val filtered: DataFrame =
      if (where != null && where.nonEmpty) df.filter(where) else df
    val rows = filtered.collect()  // Row[] \u2014 Array[Row]
    new CachedResult(rows, filtered.schema)
  }

  /** Convert a {@link CachedResult} to the platform's
    * {@code List<List<Object>>} positional wire shape, with each
    * row projected in schema-declaration order.
    */
  def rowsAsJava(cached: CachedResult): java.util.List[java.util.List[AnyRef]] = {
    val out = new java.util.ArrayList[java.util.List[AnyRef]](cached.rows.length)
    var i = 0
    while (i < cached.rows.length) {
      val r = cached.rows(i)
      val row = new java.util.ArrayList[AnyRef](r.size)
      var j = 0
      while (j < r.size) {
        if (r.isNullAt(j)) row.add(null)
        else row.add(r.get(j).asInstanceOf[AnyRef])
        j += 1
      }
      out.add(row)
      i += 1
    }
    out
  }

  /** Schema field names \u2192 {@code java.util.List[String>}. Preserves
    * declaration order (part of the result-shape contract).
    */
  def schemaFieldsAsJava(cached: CachedResult): java.util.List[String] = {
    val fields = cached.schema.fields
    val out = new java.util.ArrayList[String](fields.length)
    var i = 0
    while (i < fields.length) {
      out.add(fields(i).name)
      i += 1
    }
    out
  }

  /** Build the library's {@link QueryRequest} for use with
    * {@code CacheKey.forRequest}. Defaults the where/having/time-grain
    * fields to empty (the platform wire DTO doesn't yet carry them).
    */
  def buildQueryRequest(
      modelName: String,
      modelVersion: Int,
      measures: java.util.List[String],
      dimensions: java.util.List[String],
  ): QueryRequest = new QueryRequest(
    model      = modelName,
    version    = modelVersion,
    measures   = if (measures != null) measures.asScala.toSeq else Seq.empty,
    dimensions = if (dimensions != null) dimensions.asScala.toSeq else Seq.empty,
  )

  /** Java-friendly: model.name().getOrElse("unknown") without
    * the Option.getOrElse(...) overload ambiguity that bites
    * Java callers. */
  def modelNameOrUnknown(model: SemanticTable): String =
    model.name.getOrElse("unknown")
}
