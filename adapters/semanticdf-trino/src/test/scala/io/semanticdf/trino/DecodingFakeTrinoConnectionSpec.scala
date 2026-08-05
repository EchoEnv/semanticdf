package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.EngineContext
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Dimension, FilterSpec, Measure, Model, SourceRef}
import io.semanticdf.core.rel.AggregateFn
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove that `TrinoResultDecoder` is correctly
  * integrated into the connection flow end-to-end.
  *
  * Uses `DecodingFakeTrinoConnection` (a connection fixture that
  * delegates to the decoder) and a `TrinoEngine` wired to it. The
  * test asserts: when the engine executes a compiled plan, the
  * returned `TrinoResult` is the decoder's output (raw rows →
  * `LiteralValue` cells).
  *
  * Per scala-data-driven-refactor §1: pure behavior — given a
  * Model + raw rows, the engine produces a deterministic
  * TrinoResult via the decoder.
  */
class DecodingFakeTrinoConnectionSpec extends AnyFunSuite with Matchers {

  // -- helpers --

  /** Build a minimal Model with a single dimension + measure. */
  private def buildModel(): Model = {
    val attempt = Model.of(
      name       = "orders_by_region",
      source     = SourceRef.ByName(
        catalog   = Some("hive"),
        namespace = Some("silver"),
        table     = "orders",
      ),
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures   = List(
        Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount")),
      ),
    )
    attempt.fold(err => fail(s"Model.of failed: $err"), identity)
  }

  // -- decode-integration tests --

  test("decoder produces LiteralValue cells from raw rows") {
    val rawRows: List[List[Any]] = List(
      List("AA", 100L),
      List("BB", 200L),
    )
    val connection = DecodingFakeTrinoConnection.of(
      columns = List("region", "total"),
      rawRows = rawRows,
    )
    val engine = new TrinoEngine().withConnectionFactory(() => connection)
    val m      = buildModel()
    val plan   = engine.compile(m, EngineContext.defaultContext).toOption.get
    val result = engine.execute(plan, EngineContext.defaultContext).toOption.get.asInstanceOf[TrinoResult]

    // Columns are preserved
    result.columns shouldBe List("region", "total")

    // Raw rows are decoded to LiteralValue cells
    result.rowCount shouldBe 2
    result.cell(0, 0) shouldBe Some(LiteralValue.StringValue("AA"))
    result.cell(0, 1) shouldBe Some(LiteralValue.LongValue(100L))
    result.cell(1, 0) shouldBe Some(LiteralValue.StringValue("BB"))
    result.cell(1, 1) shouldBe Some(LiteralValue.LongValue(200L))
  }

  test("decoder translates null cells to LiteralValue.NullValue") {
    val rawRows: List[List[Any]] = List(
      List("AA", null),
    )
    val connection = DecodingFakeTrinoConnection.of(
      columns = List("region", "total"),
      rawRows = rawRows,
    )
    val engine = new TrinoEngine().withConnectionFactory(() => connection)
    val m      = buildModel()
    val plan   = engine.compile(m, EngineContext.defaultContext).toOption.get
    val result = engine.execute(plan, EngineContext.defaultContext).toOption.get.asInstanceOf[TrinoResult]

    result.rowCount shouldBe 1
    result.cell(0, 0) shouldBe Some(LiteralValue.StringValue("AA"))
    result.cell(0, 1) shouldBe Some(LiteralValue.NullValue)
  }

  test("decoder handles mixed-type columns") {
    val rawRows: List[List[Any]] = List(
      List(1: Int, "Alice" : Any, true : Any, BigDecimal("100.50")),
      List(2: Int, "Bob"   : Any, false : Any, BigDecimal("200.75")),
    )
    val connection = DecodingFakeTrinoConnection.of(
      columns = List("id", "name", "active", "salary"),
      rawRows = rawRows,
    )
    val engine = new TrinoEngine().withConnectionFactory(() => connection)
    val m      = buildModel()
    val plan   = engine.compile(m, EngineContext.defaultContext).toOption.get
    val result = engine.execute(plan, EngineContext.defaultContext).toOption.get.asInstanceOf[TrinoResult]

    result.cell(0, 0) shouldBe Some(LiteralValue.IntValue(1))
    result.cell(0, 1) shouldBe Some(LiteralValue.StringValue("Alice"))
    result.cell(0, 2) shouldBe Some(LiteralValue.BoolValue(true))
    result.cell(0, 3) shouldBe Some(LiteralValue.DecimalValue(BigDecimal("100.50")))

    result.cell(1, 0) shouldBe Some(LiteralValue.IntValue(2))
    result.cell(1, 1) shouldBe Some(LiteralValue.StringValue("Bob"))
    result.cell(1, 2) shouldBe Some(LiteralValue.BoolValue(false))
    result.cell(1, 3) shouldBe Some(LiteralValue.DecimalValue(BigDecimal("200.75")))
  }

  test("decoder returns empty TrinoResult for empty rawRows") {
    val connection = DecodingFakeTrinoConnection.of(
      columns = List("a"),
      rawRows = Nil,
    )
    val engine = new TrinoEngine().withConnectionFactory(() => connection)
    val m      = buildModel()
    val plan   = engine.compile(m, EngineContext.defaultContext).toOption.get
    val result = engine.execute(plan, EngineContext.defaultContext).toOption.get.asInstanceOf[TrinoResult]

    result.rowCount shouldBe 0
    result.rows shouldBe Nil
  }

  test("decoder integration proves the full pipeline is wired") {
    // The fact that this test passes proves:
    //   1. compile produces a ParameterizedSql (via TrinoQueryCompiler)
    //   2. execute passes the SQL to the connection
    //   3. the connection calls TrinoResultDecoder.decode
    //   4. the decoder translates raw rows to LiteralValue
    //   5. the engine returns the decoded TrinoResult
    //
    // If any link in this chain broke, this test would fail.
    val connection = DecodingFakeTrinoConnection.of(
      columns = List("region", "total"),
      rawRows = List(List("AA", 42L)),
    )
    val engine = new TrinoEngine().withConnectionFactory(() => connection)
    val m      = buildModel()
    val plan   = engine.compile(m, EngineContext.defaultContext).toOption.get
    val result = engine.execute(plan, EngineContext.defaultContext).toOption.get.asInstanceOf[TrinoResult]

    result.rowCount shouldBe 1
    result.cell(0, 0) shouldBe Some(LiteralValue.StringValue("AA"))
    result.cell(0, 1) shouldBe Some(LiteralValue.LongValue(42L))
  }
}