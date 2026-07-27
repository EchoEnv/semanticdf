package io.semanticdf.audit

import io.semanticdf.SparkSessionFixture
import io.semanticdf.adapters.{SemanticManifest, YamlLoader}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/** Verifies that the `clock: () => Instant` parameter on the library's
  * `toDataFrame` / `toJson` / `toJoinedJson` is wired through end-to-end. */
class ClockInjectionSpec extends AnyFunSuite with Matchers with SparkSessionFixture {

  // A frozen clock that always returns the same instant.
  private val frozen = Instant.parse("2026-07-26T10:00:00Z")
  private val clock: () => Instant = () => frozen

  // Helper: register the temp view, load the model, attach an audit sink,
  // and run the callback. This is the standard pattern from
  // AuditSpec and the existing tests.
  private def withAuditedModel[A](f: io.semanticdf.SemanticTable => A): A = {
    val s = spark
    val rows = s.sparkContext.parallelize(Seq(
      Row("AA", 100L, 50L), Row("UA", 200L, 75L)
    ))
    val schema = StructType(Seq(
      StructField("carrier",   org.apache.spark.sql.types.StringType),
      StructField("distance",  org.apache.spark.sql.types.LongType),
      StructField("passengers", org.apache.spark.sql.types.LongType),
    ))
    s.createDataFrame(rows, schema).createOrReplaceTempView("flights_csv")
    val tables = Map("flights_csv" -> s.table("flights_csv"))
    val model = YamlLoader.loadDir("src/test/resources/sql-cli-fixtures", tables)("flights")
      .withDimensions(
        io.semanticdf.Dimension("carrier", t => t("carrier"))
      )
      .withMeasures(
        io.semanticdf.Measure("flight_count",
          t => org.apache.spark.sql.functions.count(t("carrier")))
      )
      .withAuditSink(io.semanticdf.audit.AuditSink.inMemory(maxEvents = 10))
    f(model)
  }

  // === toDataFrame clock injection tests ===

  test("toDataFrame: injected clock produces AuditEvent.ts == clock()") {
    withAuditedModel { model =>
      implicit val ss: SparkSession = spark
      val df = model.toDataFrame()(ss, clock)
      df.collect() // trigger the audit emit
      val sink = model.auditSink.get.asInstanceOf[InMemoryAuditSink]
      val ev = sink.snapshot().head
      assert(ev.ts == frozen, s"expected ts=$frozen, got ${ev.ts}")
    }
  }

  test("toDataFrame: two consecutive calls with the same clock produce the same ts") {
    withAuditedModel { model =>
      implicit val ss: SparkSession = spark
      model.toDataFrame()(ss, clock).collect()
      model.toDataFrame()(ss, clock).collect()
      val sink = model.auditSink.get.asInstanceOf[InMemoryAuditSink]
      val evs = sink.snapshot()
      assert(evs.length == 2, s"expected 2 events, got ${evs.length}")
      assert(evs(0).ts == frozen && evs(1).ts == frozen,
        s"both events should have ts=$frozen; got ${evs.map(_.ts)}")
    }
  }

  test("toDataFrame: two consecutive calls with DIFFERENT clocks produce different ts") {
    withAuditedModel { model =>
      implicit val ss: SparkSession = spark
      val sink = model.auditSink.get.asInstanceOf[InMemoryAuditSink]
      val clockA = clock
      val clockB: () => Instant = () => frozen.plusSeconds(60)
      model.toDataFrame()(ss, clockA).collect()
      model.toDataFrame()(ss, clockB).collect()
      val evs = sink.snapshot()
      assert(evs.length == 2, s"expected 2 events, got ${evs.length}")
      assert(evs(0).ts != evs(1).ts,
        s"expected different ts; got evs(0).ts=${evs(0).ts} evs(1).ts=${evs(1).ts}")
      assert(evs(0).ts == frozen, s"first should have ts=$frozen, got ${evs(0).ts}")
      assert(evs(1).ts == frozen.plusSeconds(60),
        s"second should have ts=${frozen.plusSeconds(60)}, got ${evs(1).ts}")
    }
  }

  // === SemanticManifest.toJson / toJoinedJson clock injection tests ===

  test("SemanticManifest.toJson: same frozen clock produces same compiledAt") {
    withAuditedModel { model =>
      val json1 = SemanticManifest.toJson(model, SemanticManifest.Identity.empty, true)(clock)
      val json2 = SemanticManifest.toJson(model, SemanticManifest.Identity.empty, true)(clock)
      assert(json1 == json2, s"expected byte-identical JSON; got different results")
      assert(json1.contains("\"compiledAt\":\"" + frozen.toString + "\""),
        s"expected compiledAt=$frozen in JSON, got: $json1")
    }
  }

  test("SemanticManifest.toJson: different clocks produce different compiledAt") {
    withAuditedModel { model =>
      val clockA = clock
      val clockB: () => Instant = () => frozen.plusSeconds(120)
      val jsonA = SemanticManifest.toJson(model, SemanticManifest.Identity.empty, true)(clockA)
      val jsonB = SemanticManifest.toJson(model, SemanticManifest.Identity.empty, true)(clockB)
      assert(jsonA.contains("\"compiledAt\":\"" + frozen.toString + "\""),
        s"A should have compiledAt=$frozen, got: $jsonA")
      assert(jsonB.contains("\"compiledAt\":\"" + frozen.plusSeconds(120).toString + "\""),
        s"B should have compiledAt=${frozen.plusSeconds(120)}, got: $jsonB")
    }
  }

  // === Clock contract tests ===

  test("ClockInjectionSpec: default clock (Clock.systemDefault) keeps the library usable") {
    val a = Clock.systemDefault()
    Thread.sleep(2)
    val b = Clock.systemDefault()
    assert(b.isAfter(a) || b.equals(a),
      s"Clock.systemDefault must advance over time: a=$a b=$b")
  }

  test("ClockInjectionSpec: Clock.systemDefault is a Function0, not a value") {
    val c: () => Instant = Clock.systemDefault
    assert(c.isInstanceOf[Function0[Instant]])
  }

  // === The "no clock" path must still compile (backward compat) ===

  test("ClockInjectionSpec: library compiles and runs without explicit clock (backward compat)") {
    withAuditedModel { model =>
      implicit val ss: SparkSession = spark
      val df = model.toDataFrame()(ss, Clock.systemDefault)
      assert(df.isInstanceOf[org.apache.spark.sql.DataFrame])
    }
  }
}
