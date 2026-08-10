package io.semanticdf.platform.query;

import dev.restate.sdk.Restate;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

import io.semanticdf.SemanticTable;
import io.semanticdf.cache.CacheBridge;
import io.semanticdf.cache.CacheKey;
import io.semanticdf.cache.CachedResult;
import io.semanticdf.cache.InMemoryResultCache;
import io.semanticdf.cache.ResultCache;
import io.semanticdf.platform.streaming.ModelRegistry;

import org.apache.spark.sql.SparkSession;

import java.util.List;

import scala.Option;

/**
 * QueryService â stateless query routing.
 *
 * <p>Plain {@code @Service} (stateless, not {@code VirtualObject} or
 * {@code Workflow}). Each query is a short-lived Spark action;
 * idempotency is the caller's cache key (via {@code ResultCache}),
 * not a Restate durable promise.
 *
 * <p>The cache pattern mirrors the plan-doc C.3:
 * <ol>
 *   <li><b>Model lookup</b> via {@link ModelRegistry} â deterministic,
 *       pure, no {@code Restate.run} needed.
 *   <li><b>Cache key</b> via library's {@code CacheKey.forRequest} â
 *       SHA-256 over the full request shape including model version.
 *       Auto-invalidation: a version bump produces a different cache
 *       key, so old entries become unreachable and LRU evicts them.
 *   <li><b>Cache lookup</b> via {@code ResultCache.get} â
 *       deterministic, no {@code Restate.run} needed.
 *   <li><b>Cache-miss execution</b> â the Spark {@code toDataFrame}
 *       call goes inside {@code Restate.run("query.execute", ...)} via
 *       the library's {@link CacheBridge#executeQuery} helper so a JVM
 *       crash mid-query replays the cached {@code CachedResult}
 *       without re-executing the Spark plan.
 *   <li><b>Cache populate</b> â {@code putWithModelAndVersion} tags
 *       the entry for the sidecar
 *       (no-op on the cache itself; the cache is invalidated
 *       separately by {@code ModelService.register} via
 *       {@link ResultCache#invalidateModel(String)}).
 * </ol>
 *
 * <p>Wire shape: the {@code QueryResult.rows} field is
 * {@code List<List<Object>>} â a positional row projection.
 * Typed decoding via {@code ResultDecoder[T]} is a v0.2.3+
 * improvement that would change the wire shape (see plan Â§C.3
 * Â«Locked asÂ»).
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
  // v0.3.1 Phase 4: optional engine-portable query path. When non-null
  // AND the model is registered as a `core.Model`, `runQuery` dispatches
  // here instead of the Spark-only legacy path. May be null (legacy-only).
  private final io.semanticdf.core.engine.MCPEngineRegistry engineRegistry;

  /**
   * Constructor. Used by {@link io.semanticdf.platform.PlatformApplication}
   * (composition root) which wires:
   * <ul>
   *   <li>{@link ModelRegistry} â the platform's existing registry
   *       (filesystem-loaded at startup + runtime-registered via
   *       PR-B's {@code ModelService}).
   *   <li>{@link SparkSession} â the platform's shared session
   *       (in-process or Spark Connect, per
   *       {@code SEMANTICDF_SPARK_CONNECT_URL}).
   *   <li>{@link ResultCache} â library trait. Default for P1
   *       is {@code ResultCache.NoOp} (no caching until operators
   *       wire {@code InMemoryResultCache}).
   * </ul>
   * Tests substitute their own triple via the same constructor.
   */
  public QueryService(ModelRegistry models, SparkSession spark, ResultCache cache) {
    this(models, spark, cache, null);
  }

  /**
   * v0.3.1 Phase 4: 4-arg constructor with the optional engine-portable
   * query path. When {@code engineRegistry} is non-null AND the model
   * is registered as a {@code core.Model} (via {@code getModel}), the
   * engine-portable path is used instead of the Spark-only legacy path.
   *
   * @param engineRegistry optional; may be {@code null} for legacy-only
   *                       deployments (3-arg constructor pattern)
   */
  public QueryService(
      ModelRegistry models,
      SparkSession spark,
      ResultCache cache,
      io.semanticdf.core.engine.MCPEngineRegistry engineRegistry) {
    this.models = java.util.Objects.requireNonNull(models, "models");
    this.spark = java.util.Objects.requireNonNull(spark, "spark");
    this.cache = cache == null ? ResultCache.NoOp() : cache;
    this.engineRegistry = engineRegistry;  // null is allowed (legacy-only)
  }

  /** Convenience for tests + callers without DI. */
  public static QueryService noOp(ModelRegistry models, SparkSession spark) {
    return new QueryService(models, spark, ResultCache.NoOp());
  }

  /**
   * Run a query. {@code @Service} â stateless, no per-key
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

    // v0.3.1 Phase 4: engine-portable dispatch. When an
    // MCPEngineRegistry is wired AND the model is registered as a
    // core.Model (the new `getModel` lookup), route through the
    // engine registry instead of the Spark-only legacy path. This
    // makes the platform engine-portable: any registered engine
    // (Spark today; Trino/DuckDB/PG/Hera/UC/HMS future) can serve
    // queries through this same code path.
    //
    // Cache note: the engine-portable path skips the journaled
    // InMemoryResultCache for v1 (PortableQueryResult is a
    // different shape than RestateCachedRow). Future work can
    // add a PortableQueryResult-shaped journal type. For now,
    // engine-portable queries go straight to the engine.
    if (engineRegistry != null) {
      java.util.Optional<io.semanticdf.core.model.Model> modelOpt =
          models.getModel(request.modelName());
      if (modelOpt.isPresent()) {
        return runQueryViaEngineRegistry(modelOpt.get(), request);
      }
      // No Model for this name — fall through to the legacy path.
      // (Typically means the YAML hit a ModelBridge.toModel
      // limitation at register time; the operator sees a typed
      // error in REGISTRATION_STATUS and the legacy path keeps
      // working.)
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

    // STEP 3 + STEP 4 + STEP 5: cache lookup with single-flight
    // read-through AND journaled-form caching.
    //
    // Two cache forms:
    //  - `cache.get(key)` returns the library's `CachedResult` (with
    //    Array[Row]). Used by callers that don't go through the
    //    journal (e.g. direct library users).
    //  - `cache.getJournaled(key)` returns the platform's
    //    `RestateCachedRow` (with List<Object[]> per row). The
    //    platform uses this because Restate journals this form,
    //    so caching it lets us avoid rebuilding `Array[Row]` on
    //    every cache miss (the redundant materialization that
    //    v0.2.2 paid).
    //
    // On cache MISS, the closure body:
    //  1. Runs Spark under `Restate.run("query.execute", ...)` for
    //     journal-safety (replay returns the journaled form without
    //     re-executing the Spark plan).
    //  2. Caches the journaled form via `putJournaledWithModelAndVersion`
    //     so subsequent HIT path skips the Array[Row] rebuild.
    //
    // Both happen exactly once per cache key under N concurrent
    // identical callers (single-flight in the cache layer).
    //
    // The HIT path checks `getJournaled` first; if absent (e.g.
    // legacy callers that populated via `putWithModelAndVersion`,
    // or noOp cache), falls back to `get`.
    // Only the InMemoryResultCache implementation actually supports
    // the journaled-form methods (the trait defaults are no-ops).
    // For NoOp / external implementations we fall back to the
    // library's CachedResult path (which pays the redundant
    // Array[Row] rebuild on cache miss but is correct).
    if (cache instanceof InMemoryResultCache) {
      InMemoryResultCache mem = (InMemoryResultCache) cache;
      final Object fresh = mem.getJournaled(cacheKey).isDefined() ? mem.getJournaled(cacheKey).get() : null;
      if (fresh == null) {
        final Object computed =
            mem.getOrComputeJournaled(
                cacheKey,
                () -> {
                  final RestateCachedRow journaled =
                      Restate.run(
                          "query.execute",
                          RestateCachedRow.class,
                          () -> {
                            CachedResult cr =
                                CacheBridge.executeQuery(
                                    model,
                                    spark,
                                    request.measures(),
                                    request.dimensions(),
                                    request.where());
                            return toRestateCachedRow(cr);
                          });
                  // Cache the journaled form. No rebuild to
                  // Array[Row]; the HIT path decodes directly from
                  // the journaled form to wire shape.
                  mem.putJournaledWithModelAndVersion(
                      cacheKey, journaled, request.modelName(), model.version());
                  return journaled;
                });
        return toQueryResultFromJournaled(model, (RestateCachedRow) computed);
      }
      return toQueryResultFromJournaled(model, (RestateCachedRow) fresh);
    }
    // Fallback: legacy ResultCache (NoOp / external impl).
    // Use the v0.2.2 path with the redundant rebuild — at least
    // it's correct. This branch is taken for NoOp (no cache
    // anyway) and for any future cache implementations that
    // don't override the journaled methods.
    Option<CachedResult> cached = cache.get(cacheKey);
    if (cached.isDefined()) {
      return toQueryResult(model, cached.get());
    }
    final RestateCachedRow journaled =
        Restate.run(
            "query.execute",
            RestateCachedRow.class,
            () -> {
              CachedResult cr =
                  CacheBridge.executeQuery(
                      model,
                      spark,
                      request.measures(),
                      request.dimensions(),
                      request.where());
              return toRestateCachedRow(cr);
            });
    final CachedResult rebuilt = fromRestateCachedRow(journaled);
    cache.putWithModelAndVersion(
        cacheKey, rebuilt, request.modelName(), model.version());
    return toQueryResult(model, rebuilt);
  }

  /**
   * Convert an engine-portable [[io.semanticdf.core.engine.PortableQueryResult]]
   * to the platform's wire [[QueryResult]] shape.
   *
   * <p>Per the standard: a typed conversion at the boundary. The
   * PortableQueryResult is the engine-portable shape (from core); the
   * QueryResult is the platform's wire DTO. We translate field names
   * + values row-by-row.
   *
   * <p>Per scala-error-handling §1: typed values from
   * [[io.semanticdf.core.engine.ResultValue]] are converted to Java
   * boxed values (Long, Double, String, Boolean, BigDecimal, null).
   * Maps / lists are NOT supported in the wire format (the platform
   * wire DTO is positional List&lt;Object&gt;); if the engine returns
   * a structured value, we throw IllegalArgumentException at the
   * boundary (typed error).
   */
  static QueryResult toQueryResultFromPortable(
      io.semanticdf.core.engine.PortableQueryResult portable,
      QueryRequest request) {
    // Scala collection → Java for for-each. PortableQueryResult exposes
    // Scala Vector/Seq which Java can't iterate directly.
    java.lang.Iterable<io.semanticdf.core.engine.ResultRow> scalaRows =
        scala.collection.JavaConverters.asJavaIterable(portable.rows());
    java.util.List<String> fieldNames = new java.util.ArrayList<>();
    java.util.List<java.util.List<Object>> rowsOut = new java.util.ArrayList<>();
    for (io.semanticdf.core.engine.ResultRow row : scalaRows) {
      java.util.List<Object> cells = new java.util.ArrayList<>();
      java.lang.Iterable<io.semanticdf.core.engine.ResultValue> scalaVals =
          scala.collection.JavaConverters.asJavaIterable(row.values());
      for (io.semanticdf.core.engine.ResultValue v : scalaVals) {
        cells.add(toJavaValue(v));
      }
      rowsOut.add(cells);
    }
    java.lang.Iterable<io.semanticdf.core.schema.Field> scalaFields =
        scala.collection.JavaConverters.asJavaIterable(portable.schema().fields());
    for (io.semanticdf.core.schema.Field f : scalaFields) {
      fieldNames.add(f.name());
    }
    int n = rowsOut.size();
    // Truncation flag: conservative — we don't know the cap here.
    // The MCP wire shape includes rowCount; we set truncated=false
    // because the engine returns ALL rows it computed (the cap is
    // applied upstream by the engine provider).
    return new QueryResult(
        request.modelName(),
        fieldNames,
        rowsOut,
        /*truncated*/ false,
        n);
  }

  /** Convert a single [[io.semanticdf.core.engine.ResultValue]] to a
   * Java boxed value for the platform's wire shape.
   *
   * <p>ResultValue is a Scala sealed trait; its cases are
   * {@code NullV}, {@code BoolV}, {@code IntV}, {@code DoubleV},
   * {@code DecimalV}, {@code StringV}, {@code TimestampV} (per
   * core/ResultValue). Each has a typed accessor; we dispatch by
   * class name + cast (Java has no pattern matching for sealed traits).
   *
   * <p>Structured values (lists, maps) are NOT supported in the
   * platform wire format (positional List&lt;Object&gt;). If the
   * engine returns one, we throw IllegalArgumentException at the
   * boundary (typed error per the standard). */
  private static Object toJavaValue(io.semanticdf.core.engine.ResultValue v) {
    if (v == null) {
      return null;
    }
    String simpleName = v.getClass().getSimpleName();
    switch (simpleName) {
      case "NullV$":
        return null;
      case "BoolV":
        return ((io.semanticdf.core.engine.ResultValue.BoolV) v).v();
      case "IntV":
        return ((io.semanticdf.core.engine.ResultValue.IntV) v).v();
      case "DoubleV":
        return ((io.semanticdf.core.engine.ResultValue.DoubleV) v).v();
      case "DecimalV":
        return ((io.semanticdf.core.engine.ResultValue.DecimalV) v).v();
      case "StringV":
        return ((io.semanticdf.core.engine.ResultValue.StringV) v).v();
      case "TimestampV":
        return ((io.semanticdf.core.engine.ResultValue.TimestampV) v).v().toString();
      default:
        throw new IllegalArgumentException(
            "unsupported ResultValue subtype for wire shape: " + simpleName);
    }
  }

  /**
   * v0.3.1 Phase 4: engine-portable query path.
   *
   * <p>Builds an {@link io.semanticdf.core.engine.MCPQueryRequest} from
   * the platform's wire {@link QueryRequest}, selects the default engine
   * from the {@link io.semanticdf.core.engine.MCPEngineRegistry}, and
   * delegates the query execution to that engine. The result is the
   * engine-portable {@link io.semanticdf.core.engine.PortableQueryResult}
   * which we then convert to the platform's wire {@link QueryResult}.
   *
   * <h2>Caching (Phase 4.5)</h2>
   *
   * <p>Per the design doc: the engine-portable path integrates with
   * the journaled {@link InMemoryResultCache} using the same
   * {@code RestateCachedRow} journal type as the legacy path.
   * The engine's {@link io.semanticdf.core.engine.PortableQueryResult}
   * is converted to {@link RestateCachedRow} at the boundary
   * (one-time conversion cost; same per-cell encoding as the
   * legacy path). The journal + cache pattern is identical:
   *
   * <pre>{@code
   * cache key -> cache.getJournaled(key)
   *   hit:  toQueryResultFromJournaled(name, journaled)
   *   miss: Restate.run("query.execute", RestateCachedRow.class,
   *                     () -> engine query -> toRestateCachedRowFromPortable)
   *          -> cache.putJournaled
   *          -> toQueryResultFromJournaled(name, journaled)
   * }</pre>
   *
   * <p>Per scala-jvm-safety §1: null cache key is rejected at the
   * boundary; null request is rejected earlier in {@link #runQuery}.
   *
   * <h2>Error handling</h2>
   *
   * <p>Per {@code error-handling-style.md}: engine errors are typed
   * {@link io.semanticdf.core.engine.EngineError} cases. We surface
   * them as {@link IllegalArgumentException} at the platform boundary
   * (the platform's wire protocol uses exceptions for transport; the
   * Restate handler boundary converts typed errors to the platform's
   * own error envelope).
   *
   * <h2>Engine selection</h2>
   *
   * <p>v0.3.1 P1: always the registry's default engine. The platform's
   * wire {@link QueryRequest} doesn't carry an {@code engine} field
   * (that lives on the MCP wire DTO). Future work: add an {@code engine}
   * field to the platform's wire DTO.
   */
  private QueryResult runQueryViaEngineRegistry(
      io.semanticdf.core.model.Model model, QueryRequest request) {
    // Build the MCPQueryRequest from the wire DTO. Scala's Option /
    // Seq bridge cleanly via Option.empty / Option.apply and
    // List.from (Java List → Scala immutable Seq).
    java.util.List<String> dims = request.dimensions() == null
        ? java.util.Collections.emptyList()
        : request.dimensions();
    java.util.List<String> meas = request.measures() == null
        ? java.util.Collections.emptyList()
        : request.measures();
    scala.Option<String> whereOpt = (request.where() == null || request.where().isBlank())
        ? scala.Option.empty()
        : scala.Option.apply(request.where());

    // v0.3.1 Phase C2: typed filters. The platform's wire DTO
    // has raw SQL `where` (not AST predicates), so we don't construct
    // typed FilterSpecs here — pass an empty Scala List. Future work
    // can convert the platform's wire DTO to typed filters (mirroring
    // the MCP handler in semanticdf-mcp).
    //
    // Build the empty List via the JavaConverters bridge (same
    // pattern used elsewhere in this method for dims/meas).
    java.util.List<io.semanticdf.core.model.FilterSpec> emptyFilters =
        java.util.Collections.<io.semanticdf.core.model.FilterSpec>emptyList();
    scala.collection.immutable.List<io.semanticdf.core.model.FilterSpec> filtersList =
        scala.collection.JavaConverters
            .<io.semanticdf.core.model.FilterSpec>asScalaBuffer(emptyFilters)
            .toList();

    io.semanticdf.core.engine.MCPQueryRequest mcpReq =
        new io.semanticdf.core.engine.MCPQueryRequest(
            request.modelName(),
            scala.collection.JavaConverters.asScalaBuffer(dims).toList(),
            scala.collection.JavaConverters.asScalaBuffer(meas).toList(),
            (scala.Option<Object>) scala.Option.empty(),  // limit (boxed Long)
            scala.Option.<String>empty(),  // timeGrain
            scala.Option.<scala.Tuple2<String, String>>empty(),  // timeRange
            whereOpt,
            filtersList);

    // Select the default engine. The platform wire DTO doesn't carry
    // an engine field (that's MCP-only); the registry decides via
    // its `default` field. Per `error-handling-style.md`:
    // Either[L, X] is the public API shape; we use Java's isRight
    // /right / left / get to navigate it.
    io.semanticdf.core.engine.MCPEngineProvider[] providerHolder =
        new io.semanticdf.core.engine.MCPEngineProvider[1];

    scala.util.Either<io.semanticdf.core.engine.EngineError,
                       io.semanticdf.core.engine.MCPEngineProvider> selectResult =
        // v0.3.1: callers can pin the engine via request.engine (e.g.
        // "duckdb", "postgresql:semanticdf"). When absent, fall back
        // to the registry's default (Spark in production).
        engineRegistry.select(
            (request.engine() == null || request.engine().isBlank())
                ? engineRegistry.defaultEngine()
                : request.engine());
    if (selectResult.isRight()) {
      providerHolder[0] = selectResult.right().get();
    }
    // Engine unavailable (selectResult.isLeft()) → fall back to legacy.

    if (providerHolder[0] == null) {
      // Engine unavailable — surface as IllegalArgumentException so the
      // platform's Restate handler layer can convert to its wire error
      // envelope. Callers that want a fallback should use the 3-arg
      // constructor (engineRegistry=null) which goes straight to the
      // legacy path.
      io.semanticdf.core.engine.EngineError err =
          selectResult.isLeft() ? selectResult.left().get() : null;
      throw new IllegalArgumentException(
          "engine unavailable for '" + request.modelName() + "': " + err);
    }

    // Cache key (same derivation as the legacy path so cache hits
    // are coherent across paths). The engine-portable path shares
    // the cache namespace — if a legacy execution populated the
    // cache, the engine path sees it (and vice versa).
    //
    // Per scala-jvm-safety §1: null request is rejected earlier in
    // runQuery; here we just hash the params.
    final int version = modelVersionOrZero(model);
    final String cacheKey = CacheBridge.platformCacheKey(
        request.modelName(),
        version,
        request.measures(),
        request.dimensions(),
        request.where());

    // Journaled cache path (matches the legacy pattern at lines
    // 175-201). If the cache supports journaled methods
    // (InMemoryResultCache), use them; else fall through to the
    // non-cached path.
    if (cache instanceof InMemoryResultCache) {
      final InMemoryResultCache mem = (InMemoryResultCache) cache;
      scala.Option<Object> cachedJournaled = mem.getJournaled(cacheKey);
      if (cachedJournaled.isDefined()) {
        // Cache hit: convert RestateCachedRow → QueryResult (no engine call).
        return toQueryResultFromJournaled(request.modelName(),
            (RestateCachedRow) cachedJournaled.get());
      }
      // Cache miss: execute engine + journal + cache + return.
      // Restate.run guarantees that on JVM crash + replay, we
      // DON'T re-call the engine (the journaled value is returned).
      // Per scala-spark-batch-bugs §1: closures are stateless;
      // we capture `provider`, `mcpReq`, `ctx`, `model` (all
      // serializable).
      final io.semanticdf.core.engine.MCPEngineProvider provider =
          providerHolder[0];
      final RestateCachedRow journaled = Restate.run(
          "query.execute",
          RestateCachedRow.class,
          () -> {
            scala.util.Either<io.semanticdf.core.engine.EngineError,
                               io.semanticdf.core.engine.PortableQueryResult> result;
            try {
              result = provider.query(model, mcpReq,
                  io.semanticdf.core.engine.EngineContext.defaultContext());
            } catch (RuntimeException e) {
              throw new IllegalArgumentException(
                  "engine-portable query failed for '"
                      + request.modelName() + "': " + e.getMessage(), e);
            }
            if (result.isLeft()) {
              io.semanticdf.core.engine.EngineError err = result.left().get();
              throw new IllegalArgumentException(
                  "engine error for '" + request.modelName() + "': " + err);
            }
            return toRestateCachedRowFromPortable(result.right().get());
          });
      mem.putJournaledWithModelAndVersion(
          cacheKey, journaled, request.modelName(), version);
      return toQueryResultFromJournaled(request.modelName(), journaled);
    }

    // No cache (NoOp or external impl). Execute engine directly.
    scala.util.Either<io.semanticdf.core.engine.EngineError,
                       io.semanticdf.core.engine.PortableQueryResult> either;
    try {
      either = providerHolder[0].query(
          model, mcpReq,
          io.semanticdf.core.engine.EngineContext.defaultContext());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "engine-portable query failed for '" + request.modelName()
              + "': " + e.getMessage(), e);
    }

    if (either.isRight()) {
      return toQueryResultFromPortable(either.right().get(), request);
    } else {
      // Typed engine error — surface as IllegalArgumentException at
      // the platform boundary (Restate handler layer converts to the
      // platform's wire error envelope).
      io.semanticdf.core.engine.EngineError err = either.left().get();
      throw new IllegalArgumentException(
          "engine error for '" + request.modelName() + "': " + err);
    }
  }

  /** Extract a version number from a core.Model for cache-keying.
   * Models don't carry a version field in v0.3.1 (version is a
   * legacy YAML concept); we use the model's hashCode as a stable
   * version surrogate. Two equal Models → same key (good). Different
   * Models with same content → same key (acceptable; the wire DTO
   * dominates via its content hash).
   *
   * Per scala-error-handling §1: this is a "may not exist" lookup
   * (we have a Model, not a version); we return 0 if hashing fails.
   * The cache key is just a hash bucket; collisions across versions
   * are not a correctness issue (cache hit returns the value
   * regardless). */
  private static int modelVersionOrZero(io.semanticdf.core.model.Model m) {
    if (m == null) {
      return 0;
    }
    return m.hashCode();
  }

  /**
   * v0.3.1 Phase 4: engine-portable query path.

  /**
   * Convert a {@link RestateCachedRow} directly to the platform's
   * {@link QueryResult} wire shape, bypassing the {@code Array[Row]}
   * rebuild that the v0.2.2 {@code toQueryResult(CachedResult)}
   * path required.
   *
   * <p>Performance: per cell, decode the string-encoded value back
   * to a typed Java object (the inverse of {@link #encodeCell}).
   * This is one pass over the result set — no
   * {@code Array[Row]} intermediate, no second copy of the data.
   */
  static QueryResult toQueryResultFromJournaled(
      SemanticTable model, RestateCachedRow journaled) {
    return toQueryResultFromJournaled(
        CacheBridge.modelNameOrUnknown(model), journaled);
  }

  /**
   * v0.3.1 Phase 4: engine-portable variant. Takes a model name
   * String (not a SemanticTable) because the engine-portable path
   * has a {@code core.Model}, not a {@code SemanticTable}.
   */
  static QueryResult toQueryResultFromJournaled(
      String modelName, RestateCachedRow journaled) {
    java.util.List<String> fieldNames = journaled.fieldNames();
    java.util.List<String> fieldTypes = journaled.fieldTypes();
    java.util.List<String[]> cellRows = journaled.rows();
    int n = cellRows.size();
    java.util.List<java.util.List<Object>> rows = new java.util.ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      String[] cells = cellRows.get(i);
      int cols = cells.length;
      java.util.List<Object> typed = new java.util.ArrayList<>(cols);
      for (int j = 0; j < cols; j++) {
        typed.add(decodeCell(cells[j], fieldTypes.get(j)));
      }
      rows.add(typed);
    }
    return new QueryResult(
        modelName == null ? "unknown" : modelName,
        fieldNames,
        rows,
        // Truncation flag at the real cap (the env-var-aware
        // effectiveMaxRows, falling back to CacheBridge.DefaultMaxRows
        // = 100,000), not the historical 1024 threshold.
        /*truncated*/ n >= CacheBridge.effectiveMaxRows(),
        n);
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
   * struct types are flattened to {@code String} names â the
   * platform's wire shape (QueryResult.measures) carries the
   * column names already; the schema-struct metadata is not
   * needed downstream of the cache lookup.
   */
  static RestateCachedRow toRestateCachedRow(CachedResult cached) {
    org.apache.spark.sql.Row[] rows = cached.rows();
    org.apache.spark.sql.types.StructField[] fields = cached.schema().fields();

    java.util.List<String> names = new java.util.ArrayList<>(fields.length);
    java.util.List<String> types = new java.util.ArrayList<>(fields.length);
    for (org.apache.spark.sql.types.StructField f : fields) {
      names.add(f.name());
      types.add(sparkTypeTag(f.dataType()));
    }

    java.util.List<String[]> cellRows = new java.util.ArrayList<>(rows.length);
    for (int i = 0; i < rows.length; i++) {
      org.apache.spark.sql.Row row = rows[i];
      int n = row.size();
      String[] cells = new String[n];
      for (int j = 0; j < n; j++) {
        cells[j] = encodeCell(row.isNullAt(j) ? null : row.get(j), fields[j].dataType());
      }
      cellRows.add(cells);
    }
    return new RestateCachedRow(names, types, cellRows);
  }

  /**
   * v0.3.1 Phase 4.5: convert an engine-portable
   * {@link io.semanticdf.core.engine.PortableQueryResult} to the
   * journaled {@link RestateCachedRow} shape so it can be:
   *
   * <ul>
   *   <li>Journaled via {@code Restate.run("query.execute", ...)}</li>
   *   <li>Stored in the {@link InMemoryResultCache}</li>
   *   <li>Decoded back via {@link #toQueryResultFromJournaled(String, RestateCachedRow)}</li>
   * </ul>
   *
   * <p>The cell encoding uses the same 9-tag vocabulary
   * ({@link RestateCachedRow#T_STRING}, {@link RestateCachedRow#T_LONG},
   * etc.) as the legacy Spark path, ensuring cache hits from either
   * path produce identical {@link QueryResult} shapes.
   *
   * <p>Per scala-error-handling §1: structured values (lists, maps)
   * are NOT supported in {@link RestateCachedRow}; if the engine
   * returns one, we throw {@link IllegalArgumentException} at the
   * boundary (typed error).
   */
  static RestateCachedRow toRestateCachedRowFromPortable(
      io.semanticdf.core.engine.PortableQueryResult portable) {
    // fieldNames + fieldTypes from the schema
    java.util.List<String> names = new java.util.ArrayList<>();
    java.util.List<String> types = new java.util.ArrayList<>();
    java.lang.Iterable<io.semanticdf.core.schema.Field> scalaFields =
        scala.collection.JavaConverters.asJavaIterable(portable.schema().fields());
    for (io.semanticdf.core.schema.Field f : scalaFields) {
      names.add(f.name());
      types.add(sealedTypeTag(f.dataType()));
    }

    // cellRows from each ResultRow
    java.util.List<String[]> cellRows = new java.util.ArrayList<>();
    java.lang.Iterable<io.semanticdf.core.engine.ResultRow> scalaRows =
        scala.collection.JavaConverters.asJavaIterable(portable.rows());
    for (io.semanticdf.core.engine.ResultRow row : scalaRows) {
      int n = row.values().size();
      String[] cells = new String[n];
      java.lang.Iterable<io.semanticdf.core.engine.ResultValue> scalaVals =
          scala.collection.JavaConverters.asJavaIterable(row.values());
      int j = 0;
      for (io.semanticdf.core.engine.ResultValue v : scalaVals) {
        cells[j] = encodePortableCell(v);
        j++;
      }
      cellRows.add(cells);
    }
    return new RestateCachedRow(names, types, cellRows);
  }

  /**
   * v0.3.1 Phase 4.5: map a portable {@link io.semanticdf.core.schema.SealedDataType}
   * to a {@link RestateCachedRow} tag. The tag is a closed set of
   * strings (Jackson-friendly) used by the journal/cache layer.
   */
  static String sealedTypeTag(io.semanticdf.core.schema.SealedDataType dt) {
    if (dt == null) {
      return RestateCachedRow.T_NULL;
    }
    String name = dt.getClass().getSimpleName();
    switch (name) {
      case "Varchar$":
        return RestateCachedRow.T_STRING;
      case "BigInt$": case "Int$":
        return RestateCachedRow.T_LONG;
      case "Double$":
        return RestateCachedRow.T_DOUBLE;
      case "Decimal":
        return RestateCachedRow.T_DECIMAL;
      case "Boolean$":
        return RestateCachedRow.T_BOOLEAN;
      case "Timestamp$":
        return RestateCachedRow.T_TIMESTAMP;
      case "Date$":
        return RestateCachedRow.T_DATE;
      case "Null":
        return RestateCachedRow.T_NULL;
      default:
        throw new IllegalArgumentException(
            "unsupported SealedDataType subtype for journal: " + name);
    }
  }

  /**
   * v0.3.1 Phase 4.5: encode a portable
   * {@link io.semanticdf.core.engine.ResultValue} to its string form
   * for journaling. Each case maps to a stable encoding (matching
   * the 9-tag vocabulary).
   */
  static String encodePortableCell(io.semanticdf.core.engine.ResultValue v) {
    if (v == null) {
      return null;
    }
    String simpleName = v.getClass().getSimpleName();
    switch (simpleName) {
      case "NullV$":
        return null;
      case "BoolV":
        return String.valueOf(((io.semanticdf.core.engine.ResultValue.BoolV) v).v());
      case "IntV":
        return String.valueOf(((io.semanticdf.core.engine.ResultValue.IntV) v).v());
      case "DoubleV":
        return String.valueOf(((io.semanticdf.core.engine.ResultValue.DoubleV) v).v());
      case "DecimalV":
        return ((io.semanticdf.core.engine.ResultValue.DecimalV) v).v().toString();
      case "StringV":
        return ((io.semanticdf.core.engine.ResultValue.StringV) v).v();
      case "TimestampV":
        return ((io.semanticdf.core.engine.ResultValue.TimestampV) v).v().toString();
      default:
        throw new IllegalArgumentException(
            "unsupported ResultValue subtype for journal: " + simpleName);
    }
  }

  /**
   * Map a Spark {@link org.apache.spark.sql.types.DataType} to a
   * stable string tag. The tag is a string (Jackson-friendly) and
   * stable across Spark versions because we use a closed set of
   * our own names, not Spark's class names.
   */
  static String sparkTypeTag(org.apache.spark.sql.types.DataType dt) {
    if (dt == null) return RestateCachedRow.T_NULL;
    if (dt instanceof org.apache.spark.sql.types.NullType) return RestateCachedRow.T_NULL;
    if (dt instanceof org.apache.spark.sql.types.StringType) {
      return RestateCachedRow.T_STRING;
    } else if (dt instanceof org.apache.spark.sql.types.IntegerType
        || dt instanceof org.apache.spark.sql.types.LongType
        || dt instanceof org.apache.spark.sql.types.ShortType
        || dt instanceof org.apache.spark.sql.types.ByteType) {
      return RestateCachedRow.T_LONG;
    } else if (dt instanceof org.apache.spark.sql.types.FloatType
        || dt instanceof org.apache.spark.sql.types.DoubleType) {
      return RestateCachedRow.T_DOUBLE;
    } else if (dt instanceof org.apache.spark.sql.types.DecimalType) {
      return RestateCachedRow.T_DECIMAL;
    } else if (dt instanceof org.apache.spark.sql.types.BooleanType) {
      return RestateCachedRow.T_BOOLEAN;
    } else if (dt instanceof org.apache.spark.sql.types.TimestampType) {
      return RestateCachedRow.T_TIMESTAMP;
    } else if (dt instanceof org.apache.spark.sql.types.DateType) {
      return RestateCachedRow.T_DATE;
    } else if (dt instanceof org.apache.spark.sql.types.BinaryType) {
      return RestateCachedRow.T_BINARY;
    }
    throw new IllegalArgumentException(
        "RestateCachedRow: unsupported Spark type: " + dt.getClass().getSimpleName()
        + " (nested types like ArrayType / MapType / StructType /"
        + " CalendarIntervalType are not safe to journal because Jackson"
        + " would silently coerce their content to a String. Add a new"
        + " tag in RestateCachedRow + encoder/decoder arms in QueryService"
        + " before wiring this type through the cache.)");
  }

  /**
   * Encode a Spark cell value to a string for journal-round-trip.
   * Uses Java's standard {@code toString} for most types and
   * explicit handling for the precision-sensitive ones
   * (BigDecimal, Timestamp, Date, byte[]).
   *
   * <p>Timestamp and Date use UTC-anchored representations
   * (Instant / LocalDate) so the journal survives a JVM restart in
   * a different timezone without silent wall-clock drift (the
   * Date/Timestamp landmine that produced H1).
   * fix for the DE finding C1).
   */
  static String encodeCell(Object cell, org.apache.spark.sql.types.DataType dt) {
    if (cell == null) return null;
    if (dt instanceof org.apache.spark.sql.types.DecimalType) {
      return ((java.math.BigDecimal) cell).toPlainString();
    }
    if (dt instanceof org.apache.spark.sql.types.TimestampType
        || dt instanceof org.apache.spark.sql.types.DateType) {
      // Branch on the type tag (set in sparkTypeTag) rather than
      // blindly calling cell.toString() — cell.toString() on a
      // Timestamp emits local-time text, which is wrong across
      // timezone changes.
      if (cell instanceof java.sql.Timestamp) {
        // ts.toInstant() preserves nanos; Instant.ofEpochMilli(ts.getTime())
        // would round them to millis. ISO-8601 with fractional seconds
        // round-trips through Instant.parse on the decode side.
        return ((java.sql.Timestamp) cell).toInstant().toString();
      }
      if (cell instanceof java.sql.Date) {
        return ((java.sql.Date) cell).toLocalDate().toString();
      }
    }
    if (dt instanceof org.apache.spark.sql.types.BinaryType) {
      return java.util.Base64.getEncoder().encodeToString((byte[]) cell);
    }
    if (cell instanceof java.math.BigDecimal) {
      return ((java.math.BigDecimal) cell).toPlainString();
    }
    if (cell instanceof java.sql.Timestamp) {
      // Fallback for cells typed as Timestamp but where the schema
      // tag is something else (e.g., user passed a Timestamp in
      // a non-TimestampType column). toInstant() preserves nanos.
      return ((java.sql.Timestamp) cell).toInstant().toString();
    }
    if (cell instanceof java.sql.Date) {
      // Fallback for cells typed as Date but with non-Date schema
      // tag. Use LocalDate (no timezone).
      return ((java.sql.Date) cell).toLocalDate().toString();
    }
    if (cell instanceof byte[]) {
      return java.util.Base64.getEncoder().encodeToString((byte[]) cell);
    }
    if (cell instanceof Boolean) {
      return cell.toString();
    }
    return cell.toString();
  }

  /**
   * Inverse of {@link #toRestateCachedRow}: rebuilds a
   * library-side {@link CachedResult} from a journaled
   * {@link RestateCachedRow}. Used AFTER {@code Restate.run}
   * returns so the cache layer (in-memory) and the wire shape
   * (positional rows) operate on the library's native type.
   */
  /**
   * Inverse of {@link #toRestateCachedRow}: rebuilds a library-side
   * {@link CachedResult} from a journaled {@link RestateCachedRow}.
   *
   * <p><b>LANDMINE WARNING:</b>
   * the rebuilt {@link org.apache.spark.sql.types.StructType}
   * declares every field as {@link
   * org.apache.spark.sql.types.DataTypes.StringType}. The cell
   * VALUES are correctly typed ({@link java.lang.Long}, {@link
   * java.math.BigDecimal}, {@link java.sql.Timestamp}, etc.)
   * because the {@link #decodeCell} helper consults the
   * per-column {@code fieldTypes} tag list. The schema is
   * intentionally permissive because the journal does not
   * preserve the full {@code StructType} (no nullable, no
   * metadata, no inner struct). Today's consumers read cells via
   * {@link io.semanticdf.cache.CacheBridge#rowsAsJava} (which
   * returns {@code List<List<Object>>}) and read field names via
   * {@code CacheBridge.schemaFieldsAsJava} (which returns
   * {@code List<String>}). Neither consumer consults the
   * {@code schema.types} field. As long as the wire shape stays
   * {@code List<List<Object>>} + {@code List<String> measures},
   * the StringType schema is a no-op. If a v0.2.3+ change
   * starts calling {@code cached.toDataFrame(spark)} on a
   * reconstructed {@code CachedResult}, the StringType schema
   * would coerce the typed cells back to strings — silent type
   * loss for {@code BigDecimal} precision and {@code Timestamp}
   * nanos. To guard against that, see {@code RestateCachedRow}.
   */
  static CachedResult fromRestateCachedRow(RestateCachedRow journaled) {
    int nCols = journaled.fieldNames().size();
    int nRows = journaled.rows().size();
    java.util.List<String> fieldTypes = journaled.fieldTypes();
    org.apache.spark.sql.Row[] rows = new org.apache.spark.sql.Row[nRows];
    for (int i = 0; i < nRows; i++) {
      String[] cells = journaled.rows().get(i);
      int n = cells == null ? 0 : cells.length;
      Object[] typed = new Object[n];
      for (int j = 0; j < n; j++) {
        String tag = fieldTypes.get(j);
        typed[j] = decodeCell(cells[j], tag);
      }
      rows[i] = org.apache.spark.sql.RowFactory.create(typed);
    }
    org.apache.spark.sql.types.StructField[] structFields = new org.apache.spark.sql.types.StructField[nCols];
    for (int c = 0; c < nCols; c++) {
      // We don't preserve the full StructField type info across the
      // journal; the in-memory CachedResult only needs field NAMES
      // (the platform's wire shape uses List<String> measures) and a
      // schema the platform rebuilds lazily. For the in-memory type
      // â use a permissive StringType; the next toDataFrame
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

  // (padOrTruncate removed — fromRestateCachedRow uses fieldTypes.size() to know nCols)

  /**
   * Inverse of {@link #encodeCell}. Decodes a string-encoded
   * cell back to the typed Java Object expected by Spark's Row
   * API. Throws {@code IllegalArgumentException} on unknown
   * tags (forward-compatibility break).
   */
  static Object decodeCell(String encoded, String tag) {
    if (encoded == null || RestateCachedRow.T_NULL.equals(tag)) {
      return null;
    }
    switch (tag) {
      case RestateCachedRow.T_STRING:
        return encoded;
      case RestateCachedRow.T_LONG:
        return Long.valueOf(encoded);
      case RestateCachedRow.T_DOUBLE:
        return Double.valueOf(encoded);
      case RestateCachedRow.T_DECIMAL:
        return new java.math.BigDecimal(encoded);
      case RestateCachedRow.T_BOOLEAN:
        return Boolean.valueOf(encoded);
      case RestateCachedRow.T_TIMESTAMP:
        // Encode as Instant (UTC). Timestamp.from(Instant) gives a
        // Timestamp with the same Instant regardless of JVM timezone
        // — the underlying millis are preserved.
        return java.sql.Timestamp.from(java.time.Instant.parse(encoded));
      case RestateCachedRow.T_DATE:
        // Date.getTime() must be JVM-default-timezone-independent on
        // decode. Using Date.valueOf(LocalDate.parse(s)) would
        // reconstruct a Date whose getTime() is computed at the
        // JVM-default midnight — silently shifting across JVM restarts
        // in different timezones. The fix builds the Date from an
        // Instant anchored at UTC midnight of the date. getTime()
        // returns the underlying millis (UTC midnight of the date),
        // which is JVM-timezone-independent.
        //
        // We construct a new java.sql.Date directly (not
        // Date.from(Instant), which would return java.util.Date
        // because that static method is inherited from
        // java.util.Date — the parent class). Constructing via the
        // java.sql.Date(long) constructor pins the runtime class to
        // java.sql.Date.
        return new java.sql.Date(
            java.time.LocalDate.parse(encoded)
                .atStartOfDay(java.time.ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli());
      case RestateCachedRow.T_BINARY:
        return java.util.Base64.getDecoder().decode(encoded);
      default:
        throw new IllegalArgumentException(
            "unknown RestateCachedRow type tag: " + tag);
    }
  }

  /** Convert a {@link CachedResult} to the platform's {@link QueryResult}
   * positional wire shape (delegates to the library's {@link CacheBridge}
   * for the Scala-2.13 Row Ã¢ÂÂ Java List conversion). */
  static QueryResult toQueryResult(SemanticTable model, CachedResult cached) {
    List<List<Object>> rows = rowsAsJava(cached);
    long rowCount = rows.size();
    return new QueryResult(
        CacheBridge.modelNameOrUnknown(model),
        CacheBridge.schemaFieldsAsJava(cached),
        rows,
        // Truncation flag at the real cap (the env-var-aware
        // effectiveMaxRows, falling back to CacheBridge.DefaultMaxRows
        // = 100,000), not the historical 1024 threshold. The
        // truncated flag is the only wire signal a caller has that
        // the result is incomplete; using the wrong threshold told
        // callers the result was complete when in fact we may have
        // silently dropped rows at the driver.
        /*truncated*/ rowCount >= CacheBridge.effectiveMaxRows(),
        rowCount);
  }

  /** Library-side Row Ã¢ÂÂ Java conversion (delegates to {@link CacheBridge}). */
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
      String where,
      /** Optional engine override. When non-null, routes the query to
       *  the named engine in the {@link io.semanticdf.core.engine.MCPEngineRegistry}
       *  (e.g. "spark", "duckdb", "postgresql:semanticdf"). When null,
       *  the registry's default engine is used. Added in v0.3.1 to
       *  let callers direct queries to non-default engines (DuckDB,
       *  PostgreSQL) through the same Restate ingress. */
      String engine) {}

  /** Response DTO. */
  public record QueryResult(
      String model,
      List<String> measures,
      List<List<Object>> rows,
      boolean truncated,
      long rowCount) {}
}
