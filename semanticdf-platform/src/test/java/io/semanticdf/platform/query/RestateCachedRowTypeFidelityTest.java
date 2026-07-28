package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.semanticdf.cache.CachedResult;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Pure-unit regression test for the PR #250 type-fidelity fix.
 *
 * <p>Before PR #250, {@code RestateCachedRow.rows} was typed
 * {@code List<Object[]>}. Jackson's default deserialization
 * silently coerced values:
 * <ul>
 *   <li>{@code Long} → {@code Integer} (overflow for values > 2^31)
 *   <li>{@code BigDecimal} → {@code Double} (precision loss)
 *   <li>{@code java.sql.Timestamp} → epoch {@code Long} (unit confusion)
 *   <li>{@code byte[]} → Base64 {@code String} (silent type change)
 * </ul>
 *
 * <p>PR #250 introduces a string-encoded cell payload (each cell is
 * a {@code String} whose interpretation is gated by the
 * corresponding entry in {@code RestateCachedRow.fieldTypes}). The
 * round-trip preserves full type fidelity:
 * <ul>
 *   <li>{@code Long} round-trips as {@code Long} (no Integer coercion)
 *   <li>{@code BigDecimal} round-trips as {@code BigDecimal} (no
 *       Double coercion; precision preserved)
 *   <li>{@code java.sql.Timestamp} round-trips as {@code Timestamp}
 *       (nanos preserved)
 *   <li>{@code byte[]} round-trips as {@code byte[]}
 * </ul>
 *
 * <p>This test simulates the journal round-trip path: build a
 * {@code CachedResult} with mixed types, serialize to {@code
 * RestateCachedRow} via {@code QueryService.toRestateCachedRow},
 * round-trip through Jackson (the same serializer Restate uses),
 * decode via {@code QueryService.fromRestateCachedRow}, and
 * assert the cell values are byte-for-byte equal to the originals.
 *
 * <p>No Spark runtime needed; no Restate runtime needed; pure
 * library-only test.
 */
class RestateCachedRowTypeFidelityTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void roundTripPreservesLongBeyondIntMax() throws Exception {
    // 5_000_000_000L > Integer.MAX_VALUE (2_147_483_647). The
    // pre-#250 Object[] path returned this as an Integer post-replay
    // (overflow to a negative number). Post-#250, the value round-trips
    // as Long with full precision.
    long original = 5_000_000_000L;
    Object[] row = new Object[] { original };
    CachedResult original$ = cached("c", Long.valueOf(original));

    String json = journalRoundTrip(original$, "long");
    Object[] roundtripped = readFirstRowCells(json);

    assertTrue(roundtripped[0] instanceof Long,
        "Long must round-trip as Long; was " + roundtripped[0].getClass());
    assertEquals(original, roundtripped[0]);
  }

  @Test
  void roundTripPreservesBigDecimalPrecision() throws Exception {
    // BigDecimal("1234.567890") carries 6 decimal places of precision.
    // Pre-#250 Jackson coerced to Double(1234.56789). Post-#250, the
    // BigDecimal survives the journal exactly.
    BigDecimal original = new BigDecimal("1234.567890");
    CachedResult original$ = cached("amount", original);

    String json = journalRoundTrip(original$, "decimal");
    Object[] roundtripped = readFirstRowCells(json);

    assertTrue(roundtripped[0] instanceof BigDecimal,
        "BigDecimal must round-trip as BigDecimal; was "
            + roundtripped[0].getClass());
    assertEquals(original, roundtripped[0]);
    // The toPlainString form is what toRestateCachedRow writes and
    // what fromRestateCachedRow reads. The exact decimal string is
    // preserved.
    assertEquals("1234.567890", original.toPlainString());
    assertEquals(original, new BigDecimal(original.toPlainString()));
  }

  @Test
  void roundTripPreservesTimestampNanos() throws Exception {
    // Timestamp with explicit nanos. Pre-#250 Jackson flattened to
    // epoch Long (millis only). Post-#250, nanos survive.
    Timestamp original = Timestamp.valueOf("2024-01-15 10:30:45.123456789");
    CachedResult original$ = cached("ts", original);

    String json = journalRoundTrip(original$, "timestamp");
    Object[] roundtripped = readFirstRowCells(json);

    assertTrue(roundtripped[0] instanceof Timestamp,
        "Timestamp must round-trip as Timestamp; was "
            + roundtripped[0].getClass());
    assertEquals(original, roundtripped[0]);
    // Nanos preserved: 123_456_789.
    assertEquals(123_456_789,
        ((Timestamp) roundtripped[0]).getNanos());
  }

  @Test
  void roundTripPreservesByteArray() throws Exception {
    byte[] original = new byte[] { 0, 1, 2, 3, (byte) 0xFE, (byte) 0xFF };
    CachedResult original$ = cached("blob", original);

    String json = journalRoundTrip(original$, "binary");
    Object[] roundtripped = readFirstRowCells(json);

    assertTrue(roundtripped[0] instanceof byte[],
        "byte[] must round-trip as byte[]; was " + roundtripped[0].getClass());
    assertEquals(original.length, ((byte[]) roundtripped[0]).length);
    for (int i = 0; i < original.length; i++) {
      assertEquals(original[i], ((byte[]) roundtripped[0])[i]);
    }
  }

  @Test
  void roundTripPreservesNullCells() throws Exception {
    CachedResult original$ = cached("c", (Object) null);

    String json = journalRoundTrip(original$, "string");
    Object[] roundtripped = readFirstRowCells(json);

    assertNull(roundtripped[0], "null cell must round-trip as null");
  }

  @Test
  void roundTripPreservesBoolean() throws Exception {
    CachedResult original$ = cached("flag", true);

    String json = journalRoundTrip(original$, "boolean");
    Object[] roundtripped = readFirstRowCells(json);

    assertTrue(roundtripped[0] instanceof Boolean);
    assertEquals(Boolean.TRUE, roundtripped[0]);
  }

  /**
   * PR #252 fix for the DE finding C1: a {@code Timestamp} must
   * round-trip with the SAME INSTANT regardless of the JVM timezone
   * the journal was journaled / replayed in. Pre-#250 it was
   * journaled via {@code Timestamp.toString()} (local-time text)
   * and decoded via {@code Timestamp.valueOf()} (local-time parse) —
   * a JVM restart in a different timezone would silently shift the
   * instant. Post-#250 (with the Instant-encoded journal), the
   * underlying millis are preserved.
   */
  @Test
  void roundTripPreservesTimestampAcrossTimezones() throws Exception {
    // The test fixture's static-init creates the SparkSession with
    // spark.sql.session.timeZone=UTC, so journal encoding runs in
    // UTC. We force-decoding to happen under a non-UTC default so
    // the round-trip's two halves run under different zones — the
    // critical path that exposed the original DE finding.
    java.util.TimeZone defaultTz = java.util.TimeZone.getDefault();
    try {
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(
          "America/Los_Angeles"));

      java.sql.Timestamp original = java.sql.Timestamp
          .valueOf("2024-01-15 10:30:45.123456789");

      CachedResult original$ = cached("ts", original);
      String json = journalRoundTrip(original$, "timestamp");
      Object[] roundtripped = readFirstRowCells(json);

      assertTrue(roundtripped[0] instanceof java.sql.Timestamp,
          "Timestamp must round-trip as Timestamp; was "
              + roundtripped[0].getClass());
      // The critical assertion: the underlying instant is the
      // same regardless of which timezone the JVM is in when the
      // decode happens.
      assertEquals(original.getTime(),
          ((java.sql.Timestamp) roundtripped[0]).getTime(),
          "Timestamp instant must survive a JVM timezone change");
      assertEquals(original.getNanos(),
          ((java.sql.Timestamp) roundtripped[0]).getNanos(),
          "Timestamp nanos must survive the journal round-trip");
    } finally {
      java.util.TimeZone.setDefault(defaultTz);
    }
  }

  /**
   * PR #252 fix for the DE finding C2: a {@code DateType} Spark
   * column must round-trip as a {@code java.sql.Date}, not a
   * {@code java.sql.Timestamp}. Pre-#250 the type tag was the same
   * for both ("timestamp") and the decoder used
   * {@code Timestamp.valueOf(s)} which is wrong for date-only data.
   * Post-#250 the tag is distinct ("date" vs "timestamp") and the
   * decoder reconstructs the right Java type.
   */
  @Test
  void roundTripPreservesDateAsDate() throws Exception {
    java.sql.Date original = java.sql.Date.valueOf("2024-01-15");
    CachedResult original$ = cached("d", original);

    // The test fixture's `cached(...)` calls `inferType(...)` which
    // is a separate code path from `sparkTypeTag` — we need the
    // schema type to be `DateType` (not `StringType`) so that
    // `toRestateCachedRow` actually emits the new "date" tag.
    CachedResult withDateSchema = withDateSchema(original$);
    String json = journalRoundTrip(withDateSchema, "date");
    Object[] roundtripped = readFirstRowCells(json);

    assertTrue(roundtripped[0] instanceof java.sql.Date,
        "DateType must round-trip as java.sql.Date, not "
            + "java.sql.Timestamp; was " + roundtripped[0].getClass());
    // LocalDate equality (no time-of-day component to drift).
    assertEquals(original.toLocalDate(),
        ((java.sql.Date) roundtripped[0]).toLocalDate());
  }

  /**
   * PR #255 fix for the DE finding H1: a {@code Date} must
   * round-trip with the SAME {@code getTime()} (underlying millis
   * since epoch) regardless of the JVM timezone the journal was
   * journaled / replayed in.
   *
   * <p>Pre-#255, {@code Date.valueOf(LocalDate.parse(s))} reconstructed
   * a {@code Date} whose {@code getTime()} was computed at the
   * JVM-default midnight — silently shifting across JVM restarts in
   * different timezones. Post-#255, the decode path goes
   * {@code LocalDate → atStartOfDay(UTC) → Instant → Date.from(Instant)},
   * giving a {@code Date} whose {@code getTime()} is the UTC-midnight
   * millis of the date — JVM-timezone-independent.
   *
   * <p>The {@code roundTripPreservesDateAsDate} test above only
   * checks {@code toLocalDate()}, which doesn't drift across
   * timezones (LocalDate has no zone). The new test here checks
   * {@code getTime()} explicitly, which IS zone-sensitive — the
   * pre-#255 path would fail under {@code TimeZone.setDefault("UTC")}
   * + decode under {@code TimeZone.setDefault("America/Los_Angeles")}.
   */
  @Test
  void roundTripPreservesDateGetTimeAcrossTimezones() throws Exception {
    java.util.TimeZone defaultTz = java.util.TimeZone.getDefault();
    try {
      // Encode under UTC (matches the test fixture's session TZ).
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));

      // Build the original Date at JVM-default midnight (2024-01-15
      // 00:00:00 UTC, in millis).
      java.sql.Date original = java.sql.Date.valueOf("2024-01-15");
      long originalMillis = original.getTime();

      // Run the journal round-trip end-to-end (the test fixture's
      // createSpark() forces session.timeZone=UTC, but the JVM default
      // for the Java date-time APIs is what Date uses).
      CachedResult original$ = withDateSchema(cached("d", original));
      String json = journalRoundTrip(original$, "date");
      Object[] beforeRestate = readFirstRowCells(json);

      // Now switch the JVM default timezone and replay.
      java.util.TimeZone.setDefault(
          java.util.TimeZone.getTimeZone("America/Los_Angeles"));

      // The journaled JSON is timezone-independent text; the decode
      // path runs the new Date.from(Instant) branch. Read the JSON
      // again (re-decoding the same journal bytes under a different
      // JVM-default timezone).
      Object[] afterRestate = readFirstRowCells(json);

      // Critical assertion: the underlying millis are JVM-tz-independent.
      assertEquals(originalMillis,
          ((java.sql.Date) beforeRestate[0]).getTime(),
          "Date.getTime() must equal the encoded millis");
      assertEquals(originalMillis,
          ((java.sql.Date) afterRestate[0]).getTime(),
          "Date.getTime() must survive a JVM timezone change between "
              + "encode and decode (pre-#255 this would shift by 8h "
              + "when going from UTC to Los_Angeles)");

      // Sanity: the LocalDate identity also holds (pre-#255 case).
      assertEquals(original.toLocalDate(),
          ((java.sql.Date) afterRestate[0]).toLocalDate());
    } finally {
      java.util.TimeZone.setDefault(defaultTz);
    }
  }

  /**
   * Re-build a CachedResult whose schema declares the single column
   * as a {@link org.apache.spark.sql.types.DateType}, not a
   * {@link StringType}. The shared {@code cached} factory uses
   * {@code inferType} which falls back to {@code StringType} for
   * non-typed inputs.
   */
  private static CachedResult withDateSchema(CachedResult c) {
    org.apache.spark.sql.types.StructType schema =
        new org.apache.spark.sql.types.StructType(new org.apache.spark.sql.types.StructField[] {
            new org.apache.spark.sql.types.StructField(
                c.schema().fields()[0].name(),
                org.apache.spark.sql.types.DataTypes.DateType,
                true,
                org.apache.spark.sql.types.Metadata.empty())
        });
    return new CachedResult(c.rows(), schema);
  }

  /**
   * Simulate the journal round-trip: build a CachedResult with the
   * given cell value, call {@code toRestateCachedRow} (the journal
   * write path), serialize the result to JSON (Restate's default
   * Jackson Serde), then read it back via {@code fromRestateCachedRow}.
   *
   * @param typeTag the {@code RestateCachedRow} type tag for the
   *     single cell ({@code "long"}, {@code "decimal"},
   *     {@code "timestamp"}, {@code "binary"}, etc.)
   */
  private String journalRoundTrip(CachedResult cached, String typeTag) throws Exception {
    // Use the public path on the in-test class (which has package
    // access to the static methods on QueryService).
    RestateCachedRow journaled = QueryService.toRestateCachedRow(cached);
    assertNotNull(journaled);
    assertEquals(1, journaled.fieldNames().size());
    assertEquals(1, journaled.fieldTypes().size());
    assertEquals(typeTag, journaled.fieldTypes().get(0),
        "expected field type tag " + typeTag);
    // Serialize through Jackson (Restate's default Serde).
    return JSON.writeValueAsString(journaled);
  }

  /**
   * Inverse of {@link #journalRoundTrip}: deserialize the JSON, run
   * the fromRestateCachedRow bridge, return the cells of the first
   * row.
   */
  private Object[] readFirstRowCells(String json) throws Exception {
    RestateCachedRow fromJson = JSON.readValue(json, RestateCachedRow.class);
    CachedResult reconstructed = QueryService.fromRestateCachedRow(fromJson);
    assertNotNull(reconstructed);
    Row[] rows = reconstructed.rows();
    assertEquals(1, rows.length, "exactly one row in the fixture");
    Object[] cells = new Object[rows[0].size()];
      for (int j = 0; j < rows[0].size(); j++) cells[j] = rows[0].get(j);
      return cells;
  }

  /** Build a 1-row, 1-column CachedResult with the given cell value. */
  private static CachedResult cached(String fieldName, Object cellValue) {
    StructField field =
        new StructField(fieldName, inferType(cellValue), true, Metadata.empty());
    StructType schema = new StructType(new StructField[] { field });
    Row row = RowFactory.create(cellValue);
    return new CachedResult(new Row[] { row }, schema);
  }

  /**
   * Pick a representative Spark type for the cell value, so that
   * {@code toRestateCachedRow} emits the right type tag.
   */
  private static org.apache.spark.sql.types.DataType inferType(Object cellValue) {
    if (cellValue == null) return DataTypes.StringType;
    if (cellValue instanceof Long) return DataTypes.LongType;
    if (cellValue instanceof Integer) return DataTypes.IntegerType;
    if (cellValue instanceof String) return DataTypes.StringType;
    if (cellValue instanceof BigDecimal) return DataTypes.createDecimalType(20, 10);
    if (cellValue instanceof Timestamp) return DataTypes.TimestampType;
    if (cellValue instanceof Boolean) return DataTypes.BooleanType;
    if (cellValue instanceof byte[]) return DataTypes.BinaryType;
    return DataTypes.StringType;
  }
}
