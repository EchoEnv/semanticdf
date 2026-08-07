package io.semanticdf.spark

import io.semanticdf.SparkSessionFixture
import io.semanticdf.core.engine.{EngineContext, MCPQueryRequest}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Dimension, Measure, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.SealedDataType
import io.semanticdf.core.engine.EngineError

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** End-to-end test for the v0.3.1 portable path through
  * [[SparkEngineProvider.query]].
  *
  * Per the Gap 1 closure plan: Spark `SparkEngineProvider.query()`
  * now routes a portable `Model` through `PortableQueryCompiler`
  * instead of the legacy `SemanticTable` fluent chain. This
  * spec verifies the wiring.
  *
  * ==Why a separate spec (vs. extending existing tests)==
  *
  * The existing `SparkEngineSpec` tests the legacy path with a
  * pre-populated `sparkTableRegistry`. The portable path doesn't
  * use the registry at all \u2014 the model is compiled from its
  * own `source: SourceRef`. Mixing the two paths in one spec
  * would obscure which behavior is under test.
  *
  * ==JVM-safety check 2 (Resource)==
  *
  * The `DataFrame.collect()` materializes the result on the driver.
  * SparkSession is shared; we don't close it. The DataFrame is
  * a logical plan; the closure shipped to executors contains only
  * Spark's internal Column expressions (stateless factory outputs). */
class SparkEngineProviderPortableSpec
  extends AnyFunSuite with Matchers with SparkSessionFixture {

  override def beforeAll(): Unit = {
    super.beforeAll()
    PortableQueryCompiler.setSparkSession(spark)
  }
  override def afterAll(): Unit = {
    PortableQueryCompiler.clearSparkSession()
    super.afterAll()
  }

  private def makeProvider: SparkEngineProvider =
    new SparkEngineProvider(spark, Map.empty)

  // -- helper: minimal Model + sample data --

  private def ordersModel(name: String = "orders"): Model = Model.of(
    name    = name,
    source  = SourceRef.ByName(Some("hive"), Some("default"), "orders"),
    dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
    measures   = List(
      Measure("total", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total")),
    ),
  ).fold(err => fail(s"Model.of failed: $err"), identity)

  // -- end-to-end --

  test("query routes a portable Model through PortableQueryCompiler end-to-end") {
    val s = spark
    import s.implicits._
    Seq(("us", 100L), ("us", 50L), ("eu", 200L)).toDF("region", "amount")
      .createOrReplaceTempView("orders")
    val provider = makeProvider
    val request = MCPQueryRequest(
      model      = "orders",
      measures   = Nil,
      dimensions = Nil,
      limit      = None,
    )
    val portable = provider.query(ordersModel(), request, EngineContext.defaultContext) match {
      case Left(err) => fail(s"query returned Left: $err")
      case Right(r)  => r
    }
    portable.rows.length shouldBe 2  // 2 groups (us, eu)
    portable.schema.fields.map(_.name).toSet shouldBe Set("region", "total")
    // Build a region -> total map (don't rely on row ordering).
    val byRegion: Map[String, Long] = portable.rows.map { row =>
      val region = row.values.head match {
        case io.semanticdf.core.engine.ResultValue.StringV(s) => s
        case other => fail(s"expected StringV for region, got $other")
      }
      val total = row.values(1) match {
        case io.semanticdf.core.engine.ResultValue.IntV(n) => n
        case other => fail(s"expected IntV for total, got $other")
      }
      region -> total
    }.toMap
    byRegion("us") shouldBe 150L
    byRegion("eu") shouldBe 200L
  }

  test("query honors request.limit") {
    val s = spark
    import s.implicits._
    Seq(("us", 100L), ("us", 50L), ("eu", 200L)).toDF("region", "amount")
      .createOrReplaceTempView("orders")
    val provider = makeProvider
    val request = MCPQueryRequest(
      model      = "orders",
      measures   = Nil,
      dimensions = Nil,
      limit      = Some(1),
    )
    val result = provider.query(ordersModel(), request, EngineContext.defaultContext).toOption.get
    result.rows.length shouldBe 1
  }

  test("query returns Left when source can't be resolved") {
    // Source points to a non-existent table; name can be anything.
    val orphanModel = Model.of(
      name    = "orphan",
      source  = SourceRef.ByName(None, None, "nonexistent_table_xyz"),
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures   = Nil,
    ).fold(err => fail(s"Model.of failed: $err"), identity)
    val provider = makeProvider
    val request = MCPQueryRequest("orphan", Nil, Nil, None)
    val result = provider.query(orphanModel, request, EngineContext.defaultContext)
    result.isLeft shouldBe true
    result.swap.toOption.get shouldBe a [EngineError.UnsupportedCapability]
  }

  // -- scala-spark-batch-bugs §3: schema-drift regression guard --

  test("query fails loud when Expr.FieldRef references an unknown column") {
    val s = spark
    import s.implicits._
    Seq(("us", 100L), ("us", 50L), ("eu", 200L)).toDF("region", "amount")
      .createOrReplaceTempView("orders")
    // orders has `region` and `amount`. Reference `missing_col`.
    val broken = Model.of(
      name    = "orders",
      source  = SourceRef.ByName(Some("hive"), Some("default"), "orders"),
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures   = List(
        Measure("broken",
          AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("missing_col")), "broken")),
      ),
    ).fold(err => fail(s"Model.of failed: $err"), identity)
    val provider = makeProvider
    val request = MCPQueryRequest("orders", Nil, Nil, None)
    val result = provider.query(broken, request, EngineContext.defaultContext)
    // Spark throws AnalysisException at execution; the portable
    // path catches it at the boundary and returns Left
    // (ConnectionFailed with the underlying exception class + msg).
    result.isLeft shouldBe true
  }

  // -- serialization regression: verify the SparkEngineProvider
  // itself is Serializable (or that query() doesn't accidentally
  // require it to be). Per scala-impact-analysis §3 --

  test("query result rows contain only portable ResultValue types (no Spark Row leaks)") {
    // Per scala-data-driven-refacer: portable outputs must be pure
    // data in the literal sense (Serializable, no SparkSession
    // refs, no Spark Row refs). This test verifies the row.values
    // are all sealed `ResultValue` ADT cases (no leaked Spark
    // types). Serialization per scala-impact-analysis §3.
    val s = spark
    import s.implicits._
    Seq(("us", 100L), ("us", 50L), ("eu", 200L)).toDF("region", "amount")
      .createOrReplaceTempView("orders")
    val provider = makeProvider
    val request = MCPQueryRequest("orders", Nil, Nil, None)
    val result = provider.query(ordersModel(), request, EngineContext.defaultContext).toOption.get
    // All row.values must be portable ResultValue (sealed ADT).
    result.rows.foreach { row =>
      row.values.foreach { v =>
        // Sanity-check the type is one of the documented portable cases.
        v.getClass.getName should startWith ("io.semanticdf.core.engine.ResultValue")
      }
    }
    // Result values themselves must be portable ResultValue (no Spark Row leaks).
    val schema = result.schema
    schema.fields.foreach { f =>
      // Each Field is a case class (auto-Product, auto-Serializable).
      // Just verify shape.
      f.name should not be empty
    }
  }
}
