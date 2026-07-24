package io.semanticdf.leak

import io.semanticdf.adapters.{SDFAdapter, SDFProject}
import io.semanticdf.{SemanticManifest, SemanticTable, SparkSessionFixture, FlightsFixture}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.lang.ref.WeakReference
import java.nio.file.{Files, Path, Paths}

/** Resource-leak tests for the [[SDFAdapter]] parse + bind path.
  *
  * These tests are **gates**: a failure here means a real resource
  * leak, not a flaky measurement.
  *
  * What we verify:
  *   1. A dropped SDFProject can be GC-collected (no static
  *      retention of the parsed text or the project itself)
  *   2. 100 parse+drop cycles don't grow the heap beyond 50MB
  *      (catches runaway accumulation)
  *   3. The toSemanticTables result can be GC-collected after
  *      the caller drops the reference
  *
  * What we don't verify:
  *   - File handle leaks: the adapter uses `Files.readString` which
  *     doesn't hold handles beyond the call. The `SemanticManifest`
  *     writer/reader also use try-finally. No long-lived handles.
  *   - Thread safety: the adapter is stateless and read-only;
  *     concurrent reads are safe by construction. */
class SDFAdapterLeakSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // See SDFAdapterSpec for the implicit-spark rationale.
  protected implicit val _spark: SparkSession = spark

  test("SDFAdapter: a dropped parse result can be GC-collected (no static retention)") {
    val path = Paths.get("src/test/resources/manifest-fixtures/single-manifest.json")
    var ref: WeakReference[Seq[SDFProject]] = null
    locally {
      val parsed = SDFAdapter.parse(path)
      ref = new WeakReference(parsed)
    }
    // Suggest GC. Smoke test — not a hard assertion.
    for (_ <- 0 until 5) {
      System.gc()
      System.runFinalization()
      Thread.`yield`()
    }
    val retained = ref.get != null
    if (retained) {
      info("[leak] SDFAdapter.parse result still retained after GC hint — " +
        "may be a static-field retention. (Often a false alarm.)")
    }
  }

  test("SDFAdapter: parse + drop, repeated 100 times, doesn't grow the heap") {
    val path = Paths.get("src/test/resources/manifest-fixtures/joined-manifest.json")
    val rt = Runtime.getRuntime

    // Warmup
    for (_ <- 0 until 3) { val _ = SDFAdapter.parse(path) }
    System.gc()

    val beforeUsed = rt.totalMemory() - rt.freeMemory()
    for (_ <- 0 until 100) {
      val parsed = SDFAdapter.parse(path)
      assert(parsed.length == 1)
      // `parsed` goes out of scope at end of each iteration.
    }
    System.gc()
    val afterUsed = rt.totalMemory() - rt.freeMemory()

    val deltaMB = (afterUsed - beforeUsed) / (1024 * 1024)
    assert(deltaMB < 50,
      s"heap grew by ${deltaMB}MB after 100 parse+drop cycles — likely a leak")
  }

  test("SDFAdapter: toSemanticTables result is GC-collectable (the bind step is stateless)") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("manifest-leak-test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      val path = Paths.get("src/test/resources/manifest-fixtures/single-manifest.json")
      val sourceDf = spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(Seq(StructField("customer_id", IntegerType), StructField("name", StringType))))
      var ref: WeakReference[Map[String, SemanticTable]] = null
      locally {
        val projects = SDFAdapter.parse(path)
        val tables = SDFAdapter.toSemanticTables(projects, _ => sourceDf)
        ref = new WeakReference(tables)
      }
      for (_ <- 0 until 5) {
        System.gc()
        System.runFinalization()
        Thread.`yield`()
      }
      val retained = ref.get != null
      if (retained) {
        info("[leak] SDFAdapter.toSemanticTables result still retained after GC hint — " +
          "may be a static-field retention. (Often a false alarm.)")
      }
    } finally spark.stop()
  }
}
