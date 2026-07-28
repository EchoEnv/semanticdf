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
 * QueryService — stateless query routing.
 *
 * <p>Plain {@code @Service} (stateless, not {@code VirtualObject} or
 * {@code Workflow}). Each query is a short-lived Spark action;
 * idempotency is the caller's cache key (via {@code ResultCache}),
 * not a Restate durable promise.
 *
 * <p>The cache pattern mirrors the plan-doc C.3:
 * <ol>
 *   <li><b>Model lookup</b> via {@link ModelRegistry} — deterministic,
 *       pure, no {@code Restate.run} needed.
 *   <li><b>Cache key</b> via library's {@code CacheKey.forRequest} —
 *       SHA-256 over the full request shape including model version.
 *       Auto-invalidation: a version bump produces a different cache
 *       key, so old entries become unreachable and LRU evicts them.
 *   <li><b>Cache lookup</b> via {@code ResultCache.get} —
 *       deterministic, no {@code Restate.run} needed.
 *   <li><b>Cache-miss execution</b> — the Spark {@code toDataFrame}
 *       call goes inside {@code Restate.run("query.execute", ...)} via
 *       the library's {@link CacheBridge#executeQuery} helper so a JVM
 *       crash mid-query replays the cached {@code CachedResult}
 *       without re-executing the Spark plan.
 *   <li><b>Cache populate</b> — {@code putWithModelAndVersion} tags
 *       the entry for the sidecar
 *       {@link ResultCache#invalidateByModelAndVersion(String, int)}
 *       called from {@code ModelService.register}.
 * </ol>
 *
 * <p>Wire shape: the {@code QueryResult.rows} field is
 * {@code List<List<Object>>} — a positional row projection.
 * Typed decoding via {@code ResultDecoder[T]} is a v0.2.3+
 * improvement that would change the wire shape (see plan §C.3
 * «Locked as»).
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
   *   <li>{@link ModelRegistry} — the platform's existing registry
   *       (filesystem-loaded at startup + runtime-registered via
   *       PR-B's {@code ModelService}).
   *   <li>{@link SparkSession} — the platform's shared session
   *       (in-process or Spark Connect, per
   *       {@code SEMANTICDF_SPARK_CONNECT_URL}).
   *   <li>{@link ResultCache} — library trait. Default for P1
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
   * Run a query. {@code @Service} — stateless, no per-key
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
    // result without re-executing the Spark plan.
    //
    // The journal entry uses the platform-local RestateCachedRow (a
    // Java record with a plain List<Object[]> per row) instead of
    // the library's CachedResult (which carries Array<Row>). Spark's
    // Row is abstract, and Jackson cannot deserialize it back on
    // replay â that's a real replay-safety bug we surface here via
    // this conversion at the journal boundary.
    final RestateCachedRow journaled =
        Restate.run(
            "query.execute",
            RestateCachedRow.class,
            () -> {
              CachedResult fresh =
                  CacheBridge.executeQuery(
                      model,
                      spark,
                      request.measures(),
                      request.dimensions(),
                      request.where());
              return toRestateCachedRow(fresh);
            });

    // STEP 5: cache populate. Tags the entry with (model_name, version)
    // so ModelService.register's invalidateByModelAndVersion(name, version)
    // hook drops the entry on a model version bump. The cache holds a
    // CachedResult (non-journaled in-process state); we rebuild it
    // from the journaled form on replay.
    final CachedResult fresh = fromRestateCachedRow(journaled);
    cache.putWithModelAndVersion(
        cacheKey, fresh, request.modelName(), model.version());

    return toQueryResult(model, fresh);
  }

  /**
   * Bridge the library's {@link CachedResult} (which carries
   * {@code Array<Row>}) to the platform's {@link RestateCachedRow}
   * (a plain {@code List<Object[]>} of cells per row). The
   * {@code RestateCachedRow} is what Restate journals; the
   * {@code CachedResult} is what the in-memory cache holds.
   *
   * <p>Null cells are preserved as null (Row's per-cell null tracking
   * carries through to {@code Object[]}'s null elements). Spark's
   * struct types are flattened to {@code String} names — the
   * platform's wire shape (QueryResult.measures) carries the
   * column names already; the schema-struct metadata is not
   * needed downstream of the cache lookup.
   */
  static RestateCachedRow toRestateCachedRow(CachedResult cached) {
    org.apache.spark.sql.Row[] rows = cached.rows();
    java.util.List<Object[]> cellRows = new java.util.ArrayList<>(rows.length);
    for (int i = 0; i < rows.length; i++) {
      org.apache.spark.sql.Row row = rows[i];
      int n = row.size();
      Object[] cells = new Object[n];
      for (int j = 0; j < n; j++) {
        cells[j] = row.isNullAt(j) ? null : row.get(j);
      }
      cellRows.add(cells);
    }
    org.apache.spark.sql.types.StructField[] fields = cached.schema().fields();
    java.util.List<String> names = new java.util.ArrayList<>(fields.length);
    for (org.apache.spark.sql.types.StructField f : fields) {
      names.add(f.name());
    }
    return new RestateCachedRow(names, cellRows);
  }

  /**
   * Inverse of {@link #toRestateCachedRow}: rebuilds a
   * library-side {@link CachedResult} from a journaled
   * {@link RestateCachedRow}. Used AFTER {@code Restate.run}
   * returns so the cache layer (in-memory) and the wire shape
   * (positional rows) operate on the library's native type.
   */
  static CachedResult fromRestateCachedRow(RestateCachedRow journaled) {
    int nCols = journaled.fieldNames().size();
    int nRows = journaled.rows().size();
    org.apache.spark.sql.Row[] rows = new org.apache.spark.sql.Row[nRows];
    for (int i = 0; i < nRows; i++) {
      Object[] cells = journaled.rows().get(i);
      // Cell type count must match column count; we don't enforce at
      // runtime here because the journaled form comes from a
      // trusted writer (us).
      Object[] paddedCells =
          (cells == null || cells.length == nCols)
              ? cells
              : padOrTruncate(cells, nCols);
      rows[i] = org.apache.spark.sql.RowFactory.create(paddedCells);
    }
    org.apache.spark.sql.types.StructField[] structFields = new org.apache.spark.sql.types.StructField[nCols];
    for (int c = 0; c < nCols; c++) {
      // We don't preserve the full StructField type info across the
      // journal; the in-memory CachedResult only needs field NAMES
      // (the platform's wire shape uses List<String> measures) and a
      // schema the platform rebuilds lazily. For the in-memory type
      // — use a permissive StringType; the next toDataFrame
      // rebuild does its own schema lookup from the model's
      structFields[c] =
          new org.apache.spark.sql.types.StructField(
              journaled.fieldNames().get(c),
              org.apache.spark.sql.types.DataTypes.StringType,
              /* nullable */ true,
              org.apache.spark.sql.types.Metadata$.MODULE$.empty());
    }
    org.apache.spark.sql.types.StructType schema =
        new org.apache.spark.sql.types.StructType(structFields);
    return new CachedResult(rows, schema);
  }

  /** Pad or truncate a row's cells to the expected column count. */
  private static Object[] padOrTruncate(Object[] cells, int nCols) {
    Object[] padded = new Object[nCols];
    int copy = Math.min(cells.length, nCols);
    System.arraycopy(cells, 0, padded, 0, copy);
    return padded;
  }

  /** Convert a {@link CachedResult} to the platform's {@link QueryResult}
   * positional wire shape (delegates to the library's {@link CacheBridge}
   * for the Scala-2.13 Row â Java List conversion). */
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

  /** Library-side Row â Java conversion (delegates to {@link CacheBridge}). */
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
