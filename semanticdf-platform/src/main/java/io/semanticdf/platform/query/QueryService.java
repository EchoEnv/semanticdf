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
 *       {@link ResultCache#invalidateByModelAndVersion(String, int)}
 *       called from {@code ModelService.register}.
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
    this.models = java.util.Objects.requireNonNull(models, "models");
    this.spark = java.util.Objects.requireNonNull(spark, "spark");
    this.cache = cache == null ? ResultCache.NoOp() : cache;
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
    // replay Ã¢ÂÂ that's a real replay-safety bug we surface here via
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
   * a different timezone without silent wall-clock drift (PR #252
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
   * <p><b>LANDMINE WARNING (PR #254 / v0.2.2 DE finding H7):</b>
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
        // PR #252: encode as Instant (UTC). Timestamp.from(Instant)
        // gives a Timestamp with the same Instant regardless of JVM
        // timezone — the underlying millis are preserved.
        return java.sql.Timestamp.from(java.time.Instant.parse(encoded));
      case RestateCachedRow.T_DATE:
        // PR #255 fix for the DE finding H1 (Date.getTime() was
        // JVM-default-timezone-dependent on decode).
        // PR #252 used Date.valueOf(LocalDate.parse(s)) which
        // reconstructs a Date whose getTime() is computed at the
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
        /*truncated*/ rowCount > 1024,
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
      String where) {}

  /** Response DTO. */
  public record QueryResult(
      String model,
      List<String> measures,
      List<List<Object>> rows,
      boolean truncated,
      long rowCount) {}
}
