package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;

import org.junit.jupiter.api.Test;

/**
 * Regression test for the v0.2.2 DE finding C3: nested Spark types
 * (ArrayType, MapType, StructType) must NOT silently fall through
 * to the safe-by-coincidence {@code T_STRING} tag, which would
 * corrupt the journal round-trip.
 *
 * <p>PR #253 changes {@code QueryService.sparkTypeTag} to throw an
 * {@link IllegalArgumentException} on these types with a clear
 * "add a new T_* tag + encode/decode arms" message. The throw
 * surfaces the unsupported-type case at compile-time-of-the-bug
 * rather than silently coercing cell content to a String on the
 * journal round-trip.
 *
 * <p>Pure-unit: no Spark runtime, no Restate, no Docker.
 */
class RestateCachedRowNestedTypeSafetyTest {

  @Test
  void arrayTypeThrowsIllegalArgument() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> QueryService.sparkTypeTag(
            new ArrayType(DataTypes.IntegerType, false)));
    assertTrue(ex.getMessage().contains("ArrayType"),
        "exception must name the offending type; was: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("RestateCachedRow"),
        "exception must point to the journal boundary; was: "
            + ex.getMessage());
  }

  @Test
  void mapTypeThrowsIllegalArgument() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> QueryService.sparkTypeTag(
            new MapType(DataTypes.StringType, DataTypes.IntegerType, false)));
    assertTrue(ex.getMessage().contains("MapType"),
        "exception must name the offending type; was: " + ex.getMessage());
  }

  @Test
  void structTypeThrowsIllegalArgument() {
    IllegalArgumentException ex = assertThrows(
        IllegalArgumentException.class,
        () -> QueryService.sparkTypeTag(
            new StructType(new org.apache.spark.sql.types.StructField[] {
                new org.apache.spark.sql.types.StructField(
                    "x", DataTypes.IntegerType, true,
                    org.apache.spark.sql.types.Metadata.empty())
            })));
    assertTrue(ex.getMessage().contains("StructType"),
        "exception must name the offending type; was: " + ex.getMessage());
  }

  @Test
  void flatTypesStillWorkAfterChange() {
    // The PR's added throw must NOT break the existing flat-type
    // support \u2014 these are the types QueryService.runQuery actually
    // emits today.
    assertTrue("null".equals(QueryService.sparkTypeTag(DataTypes.NullType)));
    assertTrue("string".equals(QueryService.sparkTypeTag(DataTypes.StringType)));
    assertTrue("long".equals(QueryService.sparkTypeTag(DataTypes.LongType)));
    assertTrue("double".equals(QueryService.sparkTypeTag(DataTypes.DoubleType)));
    assertTrue("decimal".equals(QueryService.sparkTypeTag(
        DataTypes.createDecimalType(20, 6))));
    assertTrue("boolean".equals(QueryService.sparkTypeTag(DataTypes.BooleanType)));
    assertTrue("timestamp".equals(
        QueryService.sparkTypeTag(DataTypes.TimestampType)));
    assertTrue("binary".equals(QueryService.sparkTypeTag(DataTypes.BinaryType)));
  }

  /** Helper: ensure {@code null} also returns the null tag (no null-DataType). */
  @Test
  void nullTypeReturnsNullTag() {
    // The {@code sparkTypeTag} method's first guard returns
    // {@code T_NULL} for a null input; this preserves the existing
    // behavior of {@code toRestateCachedRow} when given a null
    // schema field. Same handling for {@code NullType} explicitly
    // (PR #253 fix).
    assertTrue("null".equals(QueryService.sparkTypeTag(null)));
    assertTrue("null".equals(
        QueryService.sparkTypeTag(DataTypes.NullType)));
  }
}
