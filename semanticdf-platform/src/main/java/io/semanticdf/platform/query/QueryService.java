package io.semanticdf.platform.query;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

import io.semanticdf.SemanticTable;
import io.semanticdf.cache.CacheBridge;
import io.semanticdf.cache.CacheKey;
import io.semanticdf.cache.CachedResult;
import io.semanticdf.cache.ResultCache;
import io.semanticdf.platform.streaming.ModelRegistry;

import org.apache.spark.sql.SparkSession;

import java.util.List;

import scala.Option;

/**
 * QueryService \u2014 stateless query routing.
 *
 * <p>Plain {@code @Service} (stateless, not {@code VirtualObject} or
 * {@code Workflow}). Each query is a short-lived Spark action;
 * idempotency is the caller's cache key (via {@code ResultCache}),
 * not a Restate durable promise.
 *
 * <p>The cache pattern mirrors the plan-doc C.3:
 * <ol>
 *   <li><b>Model lookup</b> via {@link ModelRegistry} \u2014 deterministic,
 *       pure, no {@code Restate.run} needed.
 *   <li><b>Cache key</b> via library's {@code CacheKey.forRequest} \u2014
 *       SHA-256 over the full request shape including model version.
 *       Auto-invalidation: a version bump produces a different cache
 *       key, so old entries become unreachable and LRU evicts them.
 *   <li><b>Cache lookup</b> via {@code ResultCache.get} \u2014
 *       deterministic, no {@code Restate.run} needed.
 *   <li><b>Cache-miss execution</b> \u2014 the Spark {@code toDataFrame}
 *       call goes inside {@code Restate.run("query.execute", ...)} via
 *       the library's {@link CacheBridge#executeQuery} helper so a JVM
 *       crash mid-query replays the cached {@code CachedResult}
 *       without re-executing the Spark plan.
 *   <li><b>Cache populate</b> \u2014 {@code putWithModelAndVersion} tags
 *       the entry for the sidecar
 *       {@link ResultCache#invalidateByModelAndVersion(String, int)}
 *       called from {@code ModelService.register}.
 * </ol>
 *
 * <p>Wire shape: the {@code QueryResult.rows} field is
 * {@code List<List<Object>>} \u2014 a positional row projection.
 * Typed decoding via {@code ResultDecoder[T]} is a v0.2.3+
 * improvement that would change the wire shape (see plan \u00a7C.3
 * \u00abLocked as\u00bb).
 *
 * <p>Audit: the library's audit plumbing for query events is
 * deliberately NOT wired in PR-C. The plan treats query-event
 * audit as a follow-up (the streaming emit path already covers the
 * audit-log story for v0.2.1). PR-C focuses on the cache-first
 * query path; the audit emit lands in a v0.2.3 follow-up that
 * introduces the library's {@code AuditSink} wiring on top of
 * {@code QueryService}.
 */
@Service
public class QueryService {

  private final ModelRegistry models;
  private final SparkSession spark;
  private final ResultCache cache;

  /**
   * Constructor. Used by {@link io.semanticdf.platform.PlatformApplication}
   * (composition root) which wires:
   * <ul>
   *   <li>{@link ModelRegistry} \u2014 the platform's existing registry
   *       (filesystem-loaded at startup + runtime-registered via
   *       PR-B's {@code ModelService}).
   *   <li>{@link SparkSession} \u2014 the platform's shared session
   *       (in-process or Spark Connect, per
   *       {@code SEMANTICDF_SPARK_CONNECT_URL}).
   *   <li>{@link ResultCache} \u2014 library trait. Default for P1
   *       is {@code ResultCache.NoOp} (no caching until operators
   *       wire {@code InMemoryResultCache}).
   * </ul>
   * Tests substitute their own triple via the same constructor.
   */
  public QueryService(ModelRegistry models, SparkSession spark, ResultCache cache) {
    this.models = java.util.Objects.requireNonNull(models, "models");
    this.spark = java.util.Objects.requireNonNull(spark, "spark");
    this.cache = cache == null ? ResultCache.NoOp() : cache;
  }

  /** Convenience for tests + callers without DI. */
  public static QueryService noOp(ModelRegistry models, SparkSession spark) {
    return new QueryService(models, spark, ResultCache.NoOp());
  }

  /**
   * Run a query. {@code @Service} \u2014 stateless, no per-key
   * serialization. Concurrent reads are not journaled.
   *
   * <p>Returns the canonical {@link QueryResult} positional
   * representation. Throws {@code RuntimeException} if the model is
   * not found or the Spark plan fails.
   */
  @Handler
  public QueryResult runQuery(QueryRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("QueryRequest must be non-null");
    }
    if (request.modelName() == null || request.modelName().isBlank()) {
      throw new IllegalArgumentException("modelName must be non-blank");
    }

    // STEP 1: model lookup (deterministic; pure).
    final SemanticTable model;
    try {
      model = models.get(request.modelName());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "model lookup failed for '" + request.modelName() + "': " + e.getMessage(), e);
    }

    // STEP 2: cache key. Platform-side helper that hashes the FULL
    // wire DTO including the raw SQL `where` filter, so two callers
    // with different `where` strings get different cache entries
    // (PR-C-fix-1, supersedes the library's CacheKey.forRequest
    // path which would have collapsed them).
    final String cacheKey =
        CacheBridge.platformCacheKey(
            request.modelName(),
            model.version(),
            request.measures(),
            request.dimensions(),
            request.where());

    // STEP 3: cache lookup (deterministic; pure).
    Option<CachedResult> cached = cache.get(cacheKey);
    if (cached.isDefined()) {
      return toQueryResult(model, cached.get());
    }

    // STEP 4: cache-miss execution. The Spark call goes inside
    // Restate.run(...) so a JVM crash mid-query replays the cached
    // CachedResult without re-executing the Spark plan.
    final CachedResult fresh =
        Restate.run(
            "query.execute",
            CachedResult.class,
            () -> CacheBridge.executeQuery(
                model,
                spark,
                request.measures(),
                request.dimensions(),
                request.where()));

    // STEP 5: cache populate. Tags the entry with (model_name, version)
    // so ModelService.register's invalidateByModelAndVersion(name, version)
    // hook drops the entry on a model version bump.
    cache.putWithModelAndVersion(
        cacheKey, fresh, request.modelName(), model.version());

    return toQueryResult(model, fresh);
  }

  /** Convert a {@link CachedResult} to the platform's {@link QueryResult}
   * positional wire shape (delegates to the library's {@link CacheBridge}
   * for the Scala-2.13 Row → Java List conversion). */
  static QueryResult toQueryResult(SemanticTable model, CachedResult cached) {
    List<List<Object>> rows = rowsAsJava(cached);
    long rowCount = rows.size();
    return new QueryResult(
        CacheBridge.modelNameOrUnknown(model),
        CacheBridge.schemaFieldsAsJava(cached),
        rows,
        /*truncated*/ rowCount > 1024,
        rowCount);
  }

  /** Library-side Row → Java conversion (delegates to {@link CacheBridge}). */
  static List<List<Object>> rowsAsJava(CachedResult cached) {
    java.util.List<java.util.List<Object>> raw =
        (java.util.List<java.util.List<Object>>) (java.util.List<?>) CacheBridge.rowsAsJava(cached);
    @SuppressWarnings("unchecked")
    List<List<Object>> out = (List<List<Object>>) (List<?>) raw;
    return out;
  }

  /** Request DTO for {@link #runQuery(QueryRequest)}. */
  public record QueryRequest(
      String modelName,
      List<String> measures,
      List<String> dimensions,
      String where) {}

  /** Response DTO. */
  public record QueryResult(
      String model,
      List<String> measures,
      List<List<Object>> rows,
      boolean truncated,
      long rowCount) {}
}
