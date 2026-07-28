package io.semanticdf.platform.query;

import java.io.Serializable;
import java.util.List;

/**
 * A purely-data record mirror of the library's
 * {@link io.semanticdf.cache.CachedResult} for use across a
 * Restate journal boundary.
 *
 * <p>Why this exists: the library's {@code CachedResult} carries
 * {@code Array[org.apache.spark.sql.Row]}, but the Restate SDK's
 * default Jackson serializer can WRITE {@code Row} values via
 * {@code GenericRowWithSchema} but cannot READ them back on journal
 * replay — {@code Row} is an abstract Spark class with no
 * default constructor. End-to-end tests showed the deserialization
 * throws {@code InvalidDefinitionException: Cannot construct
 * instance of org.apache.spark.sql.Row}.
 *
 * <p>This record is purely Jackson-friendly: {@code List<Object>}
 * per cell (no Spark types), and the field metadata as plain
 * {@code String} names. Restate can round-trip it through the
 * journal; on replay the platform's {@code QueryService}
 * converts back to {@code CachedResult} (lazily; the rebuilt
 * DataFrame re-runs the row schema lookup from
 * {@link org.apache.spark.sql.SparkSession#createDataFrame} on
 * demand).
 *
 * <p>Defined as a Java {@code record} so it round-trips cleanly
 * through Jackson out of the box.
 */
public record RestateCachedRow(
    List<String> fieldNames,
    List<Object[]> rows) implements Serializable {

  /**
   * Compact constructor — reject obviously-malformed input early.
   */
  public RestateCachedRow {
    if (fieldNames == null) {
      throw new IllegalArgumentException("fieldNames must be non-null");
    }
    if (rows == null) {
      throw new IllegalArgumentException("rows must be non-null");
    }
  }
}
