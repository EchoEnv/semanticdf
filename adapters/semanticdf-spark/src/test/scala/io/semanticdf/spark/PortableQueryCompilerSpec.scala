package io.semanticdf.spark

import io.semanticdf.SparkSessionFixture
import io.semanticdf.core.engine.{EngineContext, EngineError, MCPQueryRequest}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Dimension, FilterSpec, JoinSpec, Measure, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn, JoinKind}
import io.semanticdf.core.schema.SealedDataType

import org.apache.spark.sql.{Column, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for [[PortableQueryCompiler]] — the engine-specific Spark
  * compiler that walks a portable `Model` and emits a Spark
  * `DataFrame` (per scala-data-driven-refacer \u00a71: behavior in
  * adapters).
  *
  * ==Why a separate compiler (vs. extending TrinoQueryCompiler)==
  *
  * Trino emits SQL strings. Spark's API is typed
  * (`DataFrame.transform(...)`). Different output shape; can't share.
  *
  * ==v0.3.1 scope (Gap 1 partial closure)==
  *
  * This PR closes Gap 1 from
  * `docs/design/v0.3.1-feature-parity-backlog.md` by giving Spark
  * the ability to compile + execute a portable `Model` directly,
  * bypassing the legacy `SemanticTable` fluent chain. */
class PortableQueryCompilerSpec
  extends AnyFunSuite with Matchers with SparkSessionFixture {

  // Hook the SparkSessionFixture's lifecycle into the compiler's
  // session carrier. The compiler reads the session via
  // PortableQueryCompiler.sparkSession; we set it in beforeAll
  // and clear it in afterAll to avoid leaking across tests.
  override def beforeAll(): Unit = {
    super.beforeAll()
    PortableQueryCompiler.setSparkSession(spark)
  }
  override def afterAll(): Unit = {
    PortableQueryCompiler.clearSparkSession()
    super.afterAll()
  }

  // -- helper: build a Model with optional overrides --
  // Model is a `final class` (not case class), so no `.copy`.
  // Build via Model.of() each time.

  private def modelWith(
      filters: List[FilterSpec] = Nil,
      joins:   List[JoinSpec]   = Nil,
  ): Model = Model.of(
    name    = "orders",
    source  = SourceRef.ByName(
      catalog   = Some("hive"),
      namespace = Some("default"),
      table     = "orders",
    ),
    dimensions = List(
      Dimension.field("region", SealedDataType.Varchar),
    ),
    measures = List(
      Measure("total_amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total_amount")),
      Measure("row_count",    AggregateCall(AggregateFn.Count, None, "row_count")),
    ),
    filters = filters,
    joins   = joins,
  ).fold(err => fail(s"Model.of failed: $err"), identity)

  private def customersModel: Model = Model.of(
    name    = "customers",
    source  = SourceRef.ByName(
      catalog   = Some("hive"),
      namespace = Some("default"),
      table     = "customers",
    ),
    dimensions = Nil,
    measures   = Nil,
  ).fold(err => fail(s"Model.of failed: $err"), identity)

  // -- source resolution --

  test("compile resolves model.source (SourceRef.ByName) to a DataFrame via Spark catalog") {
    val spark = this.spark
    import spark.implicits._
    Seq(("us", 100), ("us", 50), ("eu", 200)).toDF("region", "amount")
      .createOrReplaceTempView("orders")
    // Verify source resolution succeeded (returns Right) — the
    // resulting DataFrame is grouped (2 groups), so .count() = 2.
    // The raw source has 3 rows; covered by the next test.
    new PortableQueryCompiler().compile(modelWith(), EngineContext.defaultContext)
      .isRight shouldBe true
  }

  test("compile preserves source row count when no aggregations are requested") {
    val spark = this.spark
    import spark.implicits._
    Seq(("us", 100), ("us", 50), ("eu", 200)).toDF("region", "amount")
      .createOrReplaceTempView("orders")
    val noAggModel = Model.of(
      name    = "orders",
      source  = SourceRef.ByName(Some("hive"), Some("default"), "orders"),
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures   = Nil,
    ).fold(err => fail(s"Model.of failed: $err"), identity)
    val df = new PortableQueryCompiler().compile(noAggModel, EngineContext.defaultContext).toOption.get
    df.count() shouldBe 3
  }

  test("compile returns Left when source can't be resolved") {
    val orphan = Model.of(
      name    = "orphan",
      source  = SourceRef.ByName(
        catalog = Some("hive"), namespace = Some("default"), table = "missing_table",
      ),
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures   = Nil,
    ).fold(err => fail(s"Model.of failed: $err"), identity)
    val result = new PortableQueryCompiler().compile(orphan, EngineContext.defaultContext)
    result.isLeft shouldBe true
  }

  // -- dimensions + measures --

  test("compile emits groupBy on dimensions and agg on measures") {
    val spark = this.spark
    import spark.implicits._
    Seq(("us", 100), ("us", 50), ("eu", 200)).toDF("region", "amount")
      .createOrReplaceTempView("orders")
    val df = new PortableQueryCompiler().compile(modelWith(), EngineContext.defaultContext).toOption.get
    val rows = df.collect()
    rows.length shouldBe 2
    // 2 groups: "us" (sum=150, count=2), "eu" (sum=200, count=1)
    val us = rows.find(_.getAs[String]("region") == "us").get
    us.getAs[Long]("total_amount") shouldBe 150
    us.getAs[Long]("row_count") shouldBe 2
    val eu = rows.find(_.getAs[String]("region") == "eu").get
    eu.getAs[Long]("total_amount") shouldBe 200
    eu.getAs[Long]("row_count") shouldBe 1
  }

  // -- filters --

  test("compile applies single FilterSpec predicate") {
    val spark = this.spark
    import spark.implicits._
    Seq(("us", 100), ("us", 50), ("eu", 200)).toDF("region", "amount")
      .createOrReplaceTempView("orders")
    val filtered = modelWith(
      filters = List(FilterSpec(
        name      = "us_only",
        predicate = Expr.Equal(
          Expr.FieldRef("region"),
          Expr.Literal(LiteralValue.StringValue("us"), SealedDataType.Varchar),
        ),
      )),
    )
    val df = new PortableQueryCompiler().compile(filtered, EngineContext.defaultContext).toOption.get
    val rows = df.collect()
    rows.length shouldBe 1
    rows.head.getAs[String]("region") shouldBe "us"
  }

  // -- joins --

  test("compile emits inner join with single-key ON clause") {
    val spark = this.spark
    import spark.implicits._
    Seq((1, "alice"), (2, "bob")).toDF("id", "name")
      .createOrReplaceTempView("customers")
    Seq((1, 100), (2, 50), (3, 999)).toDF("customer_id", "amount")
      .createOrReplaceTempView("orders")
    val joined = modelWith(
      joins = List(JoinSpec(
        name       = "customers_join",
        rightModel = "customers",
        kind       = JoinKind.Inner,
        // orders has `customer_id`; customers has `id`.
        keys       = List("customer_id" -> "id"),
      )),
    )
    // Build the joined model explicitly (Model is final class, no copy).
    val joinedModel = Model.of(
      name    = "orders",
      source  = SourceRef.ByName(Some("hive"), Some("default"), "orders"),
      dimensions = List(Dimension.field("name", SealedDataType.Varchar)),
      measures   = List(Measure("total_amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total_amount"))),
      joins      = List(JoinSpec(
        name       = "customers_join",
        rightModel = "customers",
        kind       = JoinKind.Inner,
        // orders has `customer_id`; customers has `id`.
        keys       = List("customer_id" -> "id"),
      )),
    ).fold(err => fail(s"Model.of failed: $err"), identity)
    val df = new PortableQueryCompiler().compile(joinedModel, EngineContext.defaultContext).toOption.get
    val rows = df.collect()
    rows.length shouldBe 2  // orders 1+2 have matching customers; 3 has no match (Inner)
    rows.map(_.getAs[String]("name")).toSet shouldBe Set("alice", "bob")
  }

  // -- Expr \u2192 Column converter (unit-level) --

  test("PortableExprCompiler.toColumn builds a Spark Column (smoke test)") {
    val col: Column = PortableExprCompiler.toColumn(
      Expr.GreaterThan(
        Expr.FieldRef("amount"),
        Expr.Literal(LiteralValue.LongValue(50), SealedDataType.Int),
      ),
    )
    col shouldBe a [Column]
  }

  test("PortableExprCompiler.toColumn fails loud on MeasureRef (subqueries not in v0.3.1)") {
    val ex = intercept[UnsupportedOperationException] {
      PortableExprCompiler.toColumn(Expr.MeasureRef("total_amount"))
    }
    ex.getMessage should include ("MeasureRef")
  }
}
