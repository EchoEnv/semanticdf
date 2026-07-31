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
  *   2. It's a thin wrapper around library primitives — nothing
  *      platform-specific.
  *
  * All methods are pure (deterministic) for a given model + request +
  * spark session; the platform wraps the JVM-side effects in
  * `Restate.run(...)` for replay safety.
  */
object CacheBridge {

  /**
   * Default hard cap on rows returned by `executeQuery`. Used by the
   * 6-arg overload to bound driver-memory usage for very large
   * results. The platform's `QueryResult.truncated` flag is computed
   * against this value (not against 1024) so the wire signal matches
   * the actual data loss.
   *
   * <p>Java callers use the [[defaultMaxRows]] accessor; Scala callers
   * use [[DefaultMaxRows]] directly.
   *
   * <p>Canonical definition lives at [[io.semanticdf.cache.CacheKey.DefaultMaxRows]];
   * this is a re-export for backward compat with code that already
   * references the platform-side constant.
   */
  @deprecated(
    "Use [[io.semanticdf.cache.CacheKey.DefaultMaxRows]] instead. " +
      "Re-export kept for binary compatibility with code compiled " +
      "before v0.2.3.",
    since = "0.2.3")
  val DefaultMaxRows: Int = io.semanticdf.cache.CacheKey.DefaultMaxRows

  /** Java-callable accessor (Java sees Scala `val`s as fields, not
   * methods, so we expose this method for clean Java syntax).
   *
   * Note: this method is NOT deprecated (it is the canonical Java API).
   * Scala callers should prefer `CacheKey.DefaultMaxRows` directly. */
  def defaultMaxRows(): Int = io.semanticdf.cache.CacheKey.DefaultMaxRows

  /** Environment variable name that operators can set to override
    * the 5-arg `executeQuery` row cap. The env-var value must be
    * a non-negative `Int`; the existing `0 = no-cap` sentinel is
    * preserved. Unset / invalid values fall back to
    * `CacheKey.DefaultMaxRows` (100,000).
    *
    * Use case: operators running a 10M-row analytical query set
    * `SEMANTICDF_MAX_ROWS=5000000` to raise the driver-memory
    * cap without changing code. The Scala constructor-time
    * `withMaxRows(n)` setter remains the per-model way. */
  val MaxRowsEnvVar: String = "SEMANTICDF_MAX_ROWS"

  /** Parse the env-var map to an effective maxRows override. Pure
    * function (extracted for testability). Returns `None` when the
    * env-var is unset, malformed, negative, or out of `Int` range.
    * `0` is accepted (the existing `0 = no-cap` sentinel). */
  def parseEnvMaxRows(env: Map[String, String]): Option[Int] =
    env.get(MaxRowsEnvVar).flatMap { s =>
      scala.util.Try(s.toInt).toOption.filter(_ >= 0)
    }

  /** Test-only override. When `Some`, takes precedence over the
    * env-var lookup. Production code never sets this; tests use it
    * to exercise the env-var path without touching the ambient
    * JVM environment. Not thread-safe — parallel test runners
    * using this var must serialize access (the project's tests
    * run sequentially). */
  private[cache] var envMaxRowsOverride: Option[Int] = None

  /** Effective maxRows: test override > env-var > library default.
    * Used by the 5-arg `executeQuery` overload and as the canonical
    * answer to "what cap is currently in effect" for callers that
    * need to know (e.g. computing a `truncated` flag on the wire).
    *
    * Java callers invoke this as `CacheBridge.effectiveMaxRows()`
    * (Scala's parameterless `def` is callable with or without
    * parens from Java; the parens style matches the existing
    * `defaultMaxRows()` accessor). */
  def effectiveMaxRows: Int =
    envMaxRowsOverride.getOrElse(parseEnvMaxRows(sys.env).getOrElse(DefaultMaxRows))


  /**
   * Run a model query against the given spark session, returning a
   * {@link CachedResult} that wraps the materialized {@code Row[]} and
   * the result {@link org.apache.spark.sql.types.StructType}.
   *
   * @param model    the compiled semantic model
   * @param spark    active SparkSession
   * @param measures column names - aggregated measures
   * @param dims     column names - group-by dimensions
   * @param where    optional Spark SQL filter expression (passed to
   *                 df.filter verbatim; structured Predicates land in
   *                 a v0.2.3 follow-up).
   * @param maxRows  hard cap on the result set. The Spark limit caps at
   *                 maxRows rows; if the underlying query returns more,
   *                 they are dropped silently at the driver. Caller
   *                 detects via rowCount == maxRows.
   */
  def executeQuery(
      model: SemanticTable,
      spark: SparkSession,
      measures: java.util.List[String],
      dims: java.util.List[String],
      where: String,
      maxRows: Int,
  ): CachedResult = {
    val df: DataFrame = model.query(measures.asScala, dims.asScala).toDataFrame(spark)
    val filtered: DataFrame =
      if (where != null && where.nonEmpty) df.filter(where) else df
    val capped: DataFrame =
      if (maxRows > 0) filtered.limit(maxRows) else filtered
    val rows = capped.collect()
    new CachedResult(rows, capped.schema)
  }

  /** Same as the 6-arg overload with a row cap that defaults to
    * `effectiveMaxRows` (the `SEMANTICDF_MAX_ROWS` env-var if set,
    * falling back to `CacheKey.DefaultMaxRows` = 100,000).
    *
    * This is a hard guard: very large results narrow at the
    * driver boundary rather than OOMing the JVM. Operators
    * override the cap via the env var (see [[MaxRowsEnvVar]]).
    * The per-model `withMaxRows(n)` setter remains the fine-grained
    * way for callers that build a specific `SemanticTable`.
    */
  def executeQuery(
      model: SemanticTable,
      spark: SparkSession,
      measures: java.util.List[String],
      dims: java.util.List[String],
      where: String,
  ): CachedResult = executeQuery(model, spark, measures, dims, where, effectiveMaxRows)

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
    *
    * NOTE: this builds a QueryRequest WITHOUT the where filter. As
    * described in {@link #platformCacheKey}, the platform's
    * {@code CacheKey.forRequest} usage would produce the same cache
    * key for two callers with different {@code where} filters. The
    * platform should prefer {@link #platformCacheKey} for cache keys;
    * this method stays for callers that want the library's canonical
    * key shape (e.g. for direct library users, not the platform).
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

  /**
   * Build a platform-side cache key from the wire QueryRequest shape.
   *
   * The library's {@code CacheKey.forRequest} hashes the library's
   * audit.QueryRequest which carries a {@code where: Option[Predicate]};
   * the platform passes the raw SQL string instead and never
   * constructs a {@code Predicate}, so {@code whereHash} would be
   * empty for every platform-built cache key. Two callers with
   * different SQL {@code where} filters would share the same key,
   * returning one caller's rows for the other's query - silent data
   * corruption.
   *
   * This helper hashes the raw wire fields directly so the SQL
   * {@code where} filter participates in the cache key (along with
   * model, version, measures, dimensions). Tests pin the contract:
   * two {@code runQuery} calls with different {@code where} strings
   * produce different keys.
   *
   * The {@code platform|v1} prefix is a version stamp - if the wire
   * shape gains a new field (e.g. orderBy in a follow-up), bump the
   * prefix to {@code v2} to invalidate every prior cache entry
   * atomically.
   */
  def platformCacheKey(
      modelName: String,
      modelVersion: Int,
      measures: java.util.List[String],
      dimensions: java.util.List[String],
      where: String,
  ): String = {
    val m  = if (modelName == null) "" else modelName
    val v  = modelVersion.toString
    val me = if (measures == null) "" else LengthPrefixed.encodeList(measures.asScala.toSeq)
    val d  = if (dimensions == null) "" else LengthPrefixed.encodeList(dimensions.asScala.toSeq)
    // `null` and empty string both mean "no where filter"; treat them
    // identically so callers passing either form get the same cache
    // key (the test `platformCacheKey_nullWhereHandledGracefully`
    // pins this contract).
    val w  = if (where == null || where.isEmpty) "" else LengthPrefixed.encodeString(where)
    val canonical = s"platform|v1|m=${LengthPrefixed.encodeString(m)}|v=$v|me=$me|d=$d|w=$w"
    LengthPrefixed.sha256(canonical)
  }

  /** Java-friendly: model.name().getOrElse("unknown") without
    * the Option.getOrElse(...) overload ambiguity that bites
    * Java callers. */
  def modelNameOrUnknown(model: SemanticTable): String =
    model.name.getOrElse("unknown")
}
