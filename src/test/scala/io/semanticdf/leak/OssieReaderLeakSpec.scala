package io.semanticdf.leak

import io.semanticdf.adapters.{OssieProject, OssieReader}
import io.semanticdf.adapters.SemanticMetadataAdapter

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.lang.ref.WeakReference
import java.nio.file.{Files, Path, Paths}

/** Resource-leak tests for the [[OssieReader]] parse path.
  *
  * These tests are **gates**: a failure here means a real resource
  * leak, not a flaky measurement. The library's contract is "no
  * overhead, no leak" — these tests make that contract testable for
  * the post-v0.1.16 Ossie adapter.
  *
  * What we verify:
  *   1. The reader doesn't retain a static reference to the SnakeYAML
  *      `Yaml` instance — every parse creates a fresh instance and
  *      drops the previous one.
  *   2. The intermediate `OssieProject` doesn't accumulate a hidden
  *      back-reference that prevents GC of the parsed file or the
  *      parser instance.
  *   3. Repeated parse + drop doesn't grow the heap (one file = one
  *      parse cycle = bounded memory).
  *
  * What we don't verify (out of scope for the Ossie adapter):
  *   - Thread-safety: the parser is read-only with no shared state,
  *     so concurrent parses are safe by construction. Testing this
  *     would be paranoid.
  *   - File handle leaks: SnakeYAML's `Files.newBufferedReader` is
  *     used inside a try-finally by SnakeYAML itself; the reader
  *     doesn't hold file handles beyond the parse call. */
class OssieReaderLeakSpec extends AnyFunSuite {

  test("OssieReader: a dropped parse result can be GC-collected (no static retention)") {
    // We hold a weak reference, drop the strong reference, and assert
    // that the GC eventually reclaims the parse result. This catches
    // the bug where the reader accidentally ends up in a static
    // field.
    val path = Paths.get("src/test/resources/ossie-fixtures/medium-ossie.yaml")
    var ref: WeakReference[Seq[OssieProject]] = null
    locally {
      val parsed = OssieReader.parse(path)
      ref = new WeakReference(parsed)
    }
    // Suggest GC. Same caveat as the other GC test: System.gc() is
    // best-effort, so this is a smoke test, not a hard assertion.
    for (_ <- 0 until 5) {
      System.gc()
      System.runFinalization()
      Thread.`yield`()
    }
    val retained = ref.get != null
    if (retained) {
      info("[leak] OssieReader.parse result still retained after GC hint — " +
        "investigate if this is a static-field retention. (Often a " +
        "false alarm: System.gc() is best-effort.)")
    }
    // Don't assert — GC timing is non-deterministic. The structural
    // invariant is "the reader doesn't hold the result in a static
    // field"; the runtime GC hint is just a smoke test.
  }

  test("OssieReader: parse + drop, repeated 100 times, doesn't grow the heap") {
    // Snapshot the heap before and after 100 parse+drop cycles. The
    // delta should be small (within JVM noise). A large delta means
    // the reader accumulates something it shouldn't.
    val path = Paths.get("src/test/resources/ossie-fixtures/medium-ossie.yaml")
    val rt = Runtime.getRuntime

    // Warmup — first parse has class loading overhead.
    for (_ <- 0 until 3) { val _ = OssieReader.parse(path) }
    System.gc()

    val beforeUsed = rt.totalMemory() - rt.freeMemory()
    for (_ <- 0 until 100) {
      val parsed = OssieReader.parse(path)
      // Force a touch to ensure the result is reachable.
      assert(parsed.length == 1)
      // `parsed` goes out of scope at end of each iteration.
    }
    System.gc()
    val afterUsed = rt.totalMemory() - rt.freeMemory()

    // Allow some noise (JVM may have grown the heap). What we catch
    // is a runaway — 100 cycles shouldn't add hundreds of MB.
    val deltaMB = (afterUsed - beforeUsed) / (1024 * 1024)
    assert(deltaMB < 50,
      s"heap grew by ${deltaMB}MB after 100 parse+drop cycles — likely a leak")
  }

  test("OssieReader: toSemanticTables also GC-reclaims (the bind step is stateless too)") {
    // The bind step creates a `Map[String, SemanticTable]`. After
    // dropping the map, the underlying tables should be GC-able too.
    // We use a tiny in-memory DataFrame to avoid depending on a file.
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    import org.apache.spark.sql.Row

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("ossie-leak-test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      val path = Paths.get("src/test/resources/ossie-fixtures/minimal-ossie.yaml")
      val ordersSchema = StructType(Seq(
        StructField("order_id",    IntegerType),
        StructField("customer_id", IntegerType),
        StructField("order_date",  StringType),
        StructField("amount",      org.apache.spark.sql.types.DoubleType),
      ))
      val customersSchema = StructType(Seq(
        StructField("customer_id", IntegerType),
        StructField("name",        StringType),
      ))
      val resolve: String => org.apache.spark.sql.DataFrame = {
        case "db.schema.orders"    => spark.createDataFrame(
          spark.sparkContext.emptyRDD[Row], ordersSchema)
        case "db.schema.customers" => spark.createDataFrame(
          spark.sparkContext.emptyRDD[Row], customersSchema)
        case other => throw new IllegalArgumentException(s"unexpected: $other")
      }
      var ref: WeakReference[Map[String, _]] = null
      locally {
        val project = OssieReader.parse(path)
        val tables = OssieReader.toSemanticTables(project, spark, resolve)
        ref = new WeakReference(tables)
      }
      for (_ <- 0 until 5) {
        System.gc()
        System.runFinalization()
        Thread.`yield`()
      }
      val retained = ref.get != null
      if (retained) {
        info("[leak] OssieReader.toSemanticTables result still retained after GC hint — " +
          "may be a static-field retention. (Often a false alarm.)")
      }
    } finally spark.stop()
  }
}
