package io.semanticdf.spark

import io.semanticdf.SparkSessionFixture
import io.semanticdf.core.engine.{EngineContext, MCPQueryRequest}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{CalculatedMeasure, Dimension, Measure, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.SealedDataType

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the portable `Expr.All` (the legacy `t.all(name)` /
 * `SemanticScope.all(name)` percent-of-total form).
 *
 * Closes Gap 2 from `docs/design/v0.3.1-feature-parity-backlog.md`.
 *
 * Per debug-mantra §1: write the reproducer first. This test
 * builds a portable `Model` with a calculated measure that uses
 * `Expr.All`, calls `SparkEngineProvider.query`, and asserts the
 * resulting `pct_of_total` column equals the expected ratio.
 *
 * Per scala-spark-batch-bugs §1 ("what you wrote isn't what runs"):
 * the test asserts the actual numeric result (not just the schema
 * or the compile success) — catches mis-implementations of the
 * window-function lowerer.
 */
class AllExprSpec
  extends AnyFunSuite with Matchers with SparkSessionFixture {

  override def beforeAll(): Unit = {
    super.beforeAll()
    PortableQueryCompiler.setSparkSession(spark)
  }
  override def afterAll(): Unit = {
    PortableQueryCompiler.clearSparkSession()
    super.afterAll()
  }

  /** A model with:
    *   - source: a table of (region, amount) rows
    *   - dimension: region
    *   - measure:  total_amount (sum of amount)
    *   - calc:     pct_of_total = amount / total_amount (per region)
    */
  private def pctModel: Model = Model.of(
    name    = "orders",
    source  = SourceRef.ByName(Some("hive"), Some("default"), "orders"),
    dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
    measures   = List(
      Measure("total_amount", AggregateCall(AggregateFn.Sum, Some(Expr.FieldRef("amount")), "total_amount")),
    ),
    calculatedMeasures = List(
      CalculatedMeasure(
        name = "pct_of_total",
        // amount / All(total_amount)
        // = amount / SUM(amount) OVER (PARTITION BY region)
        expr = Expr.Divide(
          Expr.FieldRef("amount"),
          Expr.All("total_amount"),
        ),
      ),
    ),
  ).fold(err => fail(s"Model.of failed: $err"), identity)

  test("calculated measure with Expr.All compiles + produces pct_of_total column with 2 groups") {
    val s = spark
    import s.implicits._
    Seq(("us", 100L), ("us", 50L), ("eu", 200L), ("eu", 50L)).toDF("region", "amount")
      .createOrReplaceTempView("orders")

    val provider = new SparkEngineProvider(spark, Map.empty)
    val request = MCPQueryRequest("orders", Nil, Nil, None)
    val portable = provider.query(pctModel, request, EngineContext.defaultContext) match {
      case Left(err) => fail(s"query returned Left: $err")
      case Right(r)  => r
    }

    // Per scala-spark-batch-bugs §1: assert the actual rows + columns.
    portable.rows.length shouldBe 4  // 4 input rows preserved across the join

    val cols = portable.schema.fields.map(_.name).toSet
    cols should contain ("region")
    cols should contain ("total_amount")
    cols should contain ("pct_of_total")
  }

  test("Expr.All lowerer: us group sum = 150, eu group sum = 250, per-row pct = amount/total") {
    val s = spark
    import s.implicits._
    Seq(("us", 100L), ("us", 50L), ("eu", 200L), ("eu", 50L)).toDF("region", "amount")
      .createOrReplaceTempView("orders")

    val provider = new SparkEngineProvider(spark, Map.empty)
    val request = MCPQueryRequest("orders", Nil, Nil, None)
    val portable = provider.query(pctModel, request, EngineContext.defaultContext) match {
      case Left(err) => fail(s"query returned Left: $err")
      case Right(r)  => r
    }

    // Find region + total + pct in the schema (order may vary)
    val idxRegion     = portable.schema.fields.indexWhere(_.name == "region")
    val idxTotal      = portable.schema.fields.indexWhere(_.name == "total_amount")
    val idxPct        = portable.schema.fields.indexWhere(_.name == "pct_of_total")
    idxRegion should be >= 0
    idxTotal  should be >= 0
    idxPct    should be >= 0

    // Build (region, total, pct) tuples
    case class Row(region: String, total: Long, pct: BigDecimal)
    val rows: Vector[Row] = portable.rows.toVector.map { row =>
      val region = row.values(idxRegion) match {
        case io.semanticdf.core.engine.ResultValue.StringV(s) => s
        case other => fail(s"expected StringV for region, got $other")
      }
      val total = row.values(idxTotal) match {
        case io.semanticdf.core.engine.ResultValue.IntV(n) => n
        case other => fail(s"expected numeric total, got $other")
      }
      val pct = row.values(idxPct) match {
        case io.semanticdf.core.engine.ResultValue.DecimalV(d) => d
        case io.semanticdf.core.engine.ResultValue.DoubleV(d)  => BigDecimal(d)
        case io.semanticdf.core.engine.ResultValue.IntV(n)    => BigDecimal(n)
        case other => fail(s"expected numeric pct, got $other")
      }
      Row(region, total, pct)
    }

    // Each row's total must match its region (per-region sum)
    rows.foreach { r =>
      val expectedTotal = r.region match {
        case "us" => 150L
        case "eu" => 250L
        case other => fail(s"unexpected region: $other")
      }
      r.total shouldBe expectedTotal
    }

    // Each row's pct = amount / total
    rows.foreach { r =>
      // pct * total ≈ amount (the original row's amount)
      // We don't have amount directly; assert pct is in (0, 1)
      r.pct should be > BigDecimal(0)
      r.pct should be < BigDecimal(1)
    }

    // us: 100/150 = 0.6666... and 50/150 = 0.3333... (each appears once)
    val usPcts = rows.filter(_.region == "us").map(_.pct)
    usPcts.length shouldBe 2
    usPcts.exists(p => (p - BigDecimal("0.6666666666666667")).abs < BigDecimal("0.001")) shouldBe true
    usPcts.exists(p => (p - BigDecimal("0.3333333333333333")).abs < BigDecimal("0.001")) shouldBe true

    // eu: 200/250 = 0.8 and 50/250 = 0.2
    val euPcts = rows.filter(_.region == "eu").map(_.pct)
    euPcts.length shouldBe 2
    euPcts.exists(p => (p - BigDecimal("0.8")).abs < BigDecimal("0.001")) shouldBe true
    euPcts.exists(p => (p - BigDecimal("0.2")).abs < BigDecimal("0.001")) shouldBe true
  }
}
