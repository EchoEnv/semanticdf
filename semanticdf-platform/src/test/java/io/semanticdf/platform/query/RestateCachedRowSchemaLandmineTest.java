package io.semanticdf.platform.query;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

/**
 * Regression test for the v0.2.2 DE finding H7: the {@code
 * fromRestateCachedRow} bridge reconstructs a {@link
 * org.apache.spark.sql.types.CachedResult} whose {@link
 * org.apache.spark.sql.types.StructType} declares every field as
 * {@link org.apache.spark.sql.types.DataTypes.StringType}. The
 * cell VALUES are correctly typed because {@code decodeCell}
 * consults the per-column {@code fieldTypes} tag list. The
 * schema is intentionally permissive because the journal does
 * not preserve the full {@code StructType}.
 *
 * <p>PR #254 pins the latent landmine via a Javadoc warning on
 * the {@code fromRestateCachedRow} method. If a future v0.2.3+
 * change starts calling {@code cached.toDataFrame(spark)} on a
 * reconstructed {@code CachedResult}, the StringType schema
 * would coerce the typed cells back to strings — silent type loss
 * for {@code BigDecimal} precision and {@code Timestamp} nanos.
 * The Javadoc-warning surface is the guardrail: a code reviewer
 * sees the warning when touching the file.
 *
 * <p>Pure-unit: no Spark runtime, no Restate, no Docker. The test
 * reads the source file and asserts the Javadoc is present and
 * uses the right keywords.
 */
class RestateCachedRowSchemaLandmineTest {

  @Test
  void queryService_fromRestateCachedRow_javadocWarnsAboutStringTypeSchema() throws IOException {
    String src = readQueryService();
    // Locate the fromRestateCachedRow method's Javadoc.
    int fromStart = src.indexOf("static CachedResult fromRestateCachedRow(");
    assertTrue(fromStart > 0,
        "fromRestateCachedRow(...) method must exist in QueryService.java");
    // Walk backward to find the /** that opens the Javadoc.
    int docStart = src.lastIndexOf("/**", fromStart);
    assertTrue(docStart > 0 && docStart < fromStart,
        "fromRestateCachedRow must be preceded by a /** Javadoc block");
    int docEnd = src.indexOf("*/", docStart);
    assertTrue(docEnd > 0 && docEnd < fromStart,
        "Javadoc block must be closed before the method");

    String doc = src.substring(docStart, docEnd + 2);

    // The warning must name the landmine (H7) and call out the
    // specific failure mode (silent type loss for BigDecimal /
    // Timestamp on a future cached.toDataFrame(spark) call).
    assertTrue(doc.contains("LANDMINE WARNING")
            || doc.contains("Landmine"),
        "Javadoc must explicitly mark itself as a LANDMINE warning");
    assertTrue(doc.contains("StringType"),
        "Javadoc must name StringType as the schema issue");
    assertTrue(doc.contains("BigDecimal"),
        "Javadoc must warn about BigDecimal precision loss");
    assertTrue(doc.contains("Timestamp"),
        "Javadoc must warn about Timestamp nano loss");
    assertTrue(doc.contains("toDataFrame"),
        "Javadoc must mention cached.toDataFrame as the risk surface");
  }

  @Test
  void restateCachedRow_javadocWarnsAboutPositionalTypeFidelity() throws IOException {
    // The corresponding Javadoc on the {@code RestateCachedRow}
    // class itself should also mention the trade-off: cell-type
    // fidelity is preserved, schema fidelity is not.
    String src = readRestateCachedRow();
    int classDocStart = src.indexOf("/**");
    int classDocEnd = src.indexOf("*/");
    String doc = src.substring(classDocStart, classDocEnd + 2);

    assertTrue(doc.contains("Jackson") || doc.contains("row") || doc.contains("Object"),
        "class Javadoc must mention the journal payload's data type");
  }

  /** Read the source file for the test. */
  private static String readQueryService() throws IOException {
    return readSource("semanticdf-platform/src/main/java/io/semanticdf/platform/query/QueryService.java",
        "QueryService.java");
  }

  private static String readRestateCachedRow() throws IOException {
    return readSource("semanticdf-platform/src/main/java/io/semanticdf/platform/query/RestateCachedRow.java",
        "RestateCachedRow.java");
  }

  /**
   * Read a source file relative to the project root, with fallback
   * paths for running from either the project root or the
   * semanticdf-platform/ subdirectory.
   */
  private static String readSource(String relativePath, String fallbackName) throws IOException {
    String[] candidates = {
        relativePath,
        relativePath.replace("semanticdf-platform/", ""),
        relativePath.replaceFirst("semanticdf-platform/", "../")
    };
    for (String c : candidates) {
      Path p = Paths.get(c);
      if (Files.isRegularFile(p)) {
        return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
      }
    }
    throw new AssertionError("could not locate " + fallbackName
        + " in any of: " + String.join(", ", candidates));
  }
}
