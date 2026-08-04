package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Dimension, FilterSpec, Measure, Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, AggregateFn}
import io.semanticdf.core.schema.SealedDataType

/** Phase 2 contract: prove `TrinoQueryCompiler` walks a portable
  * `Model` and emits the correct Trino SQL.
  *
  * Per scala-data-driven-refactor §1: the compiler is pure behavior
  * — given a Model, it produces a deterministic SQL string. Same
  * input → same output. No IO, no state.
  */
class TrinoQueryCompilerSpec extends AnyFunSuite with Matchers {

  private val compiler = new TrinoQueryCompiler

  // -- helpers --

  /** Build a minimal model with the given source, dimensions, measures.
    * Uses `Model.of` to validate the model is well-formed. */
  private def model(
      source:     SourceRef,
      dimensions: List[Dimension] = Nil,
      measures:   List[Measure]   = Nil,
      filters:    List[FilterSpec] = Nil,
  ): Model = {
    val attempt = Model.of(
      name       = "test_model",
      source     = source,
      dimensions = dimensions,
      measures   = measures,
      filters    = filters,
    )
    attempt.fold(err => fail(s"Model.of failed: $err"), identity)
  }

  /** Build a SourceRef.ByName with all fields. */
  private def byName: SourceRef.ByName =
    SourceRef.ByName(
      catalog   = Some("hive"),
      namespace = Some("silver"),
      table     = "orders",
    )

  // -- minimal model (just dimensions) --

  test("compile(model with one dimension) emits SELECT dim FROM source") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
    )
    val sql = compiler.compile(m)
    sql shouldBe """SELECT "region" AS "region" FROM "hive"."silver"."orders""""
  }

  // -- model with measure (triggers GROUP BY) --

  test("compile(model with dimension + measure) emits SELECT + GROUP BY") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures = List(
        Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount")),
      ),
    )
    val sql = compiler.compile(m)
    sql shouldBe """SELECT "region" AS "region", SUM("amount") AS "total" FROM "hive"."silver"."orders" GROUP BY "region""""
  }

  // -- model with filter (WHERE clause) --

  test("compile(model with filter) emits WHERE clause") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures = List(
        Measure.aggregate("total", AggregateFn.Sum, Expr.FieldRef("amount")),
      ),
      filters = List(FilterSpec(
        name      = "only_active",
        predicate = Expr.GreaterThan(Expr.FieldRef("amount"), Expr.Literal(
          LiteralValue.IntValue(0), SealedDataType.Int,
        )),
      )),
    )
    val sql = compiler.compile(m)
    sql should include (""""region" AS "region"""")
    sql should include ("""SUM("amount") AS "total"""")
    sql should include ("""WHERE (("amount" > 0))""")
    sql should include ("""GROUP BY "region"""")
  }

  // -- aggregate function variants --

  test("compile emits AVG, MIN, MAX, COUNT appropriately") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures = List(
        Measure.aggregate("avg_amt", AggregateFn.Avg, Expr.FieldRef("amount")),
        Measure.aggregate("min_amt", AggregateFn.Min, Expr.FieldRef("amount")),
        Measure.aggregate("max_amt", AggregateFn.Max, Expr.FieldRef("amount")),
      ),
    )
    val sql = compiler.compile(m)
    sql should include ("""AVG("amount") AS "avg_amt"""")
    sql should include ("""MIN("amount") AS "min_amt"""")
    sql should include ("""MAX("amount") AS "max_amt"""")
  }

  test("compile emits COUNT(*) for measure with no input") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures = List(
        Measure(name = "row_count", expr = AggregateCall(
          fn = AggregateFn.Count, input = None, alias = "row_count",
        )),
      ),
    )
    val sql = compiler.compile(m)
    sql should include ("""COUNT(*) AS "row_count"""")
  }

  test("compile emits COUNT(DISTINCT x) for distinct count") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures = List(
        Measure(name = "uniq_customers", expr = AggregateCall(
          fn = AggregateFn.Count, input = Some(Expr.FieldRef("customer_id")),
          alias = "uniq_customers", distinct = true,
        )),
      ),
    )
    val sql = compiler.compile(m)
    sql should include ("""COUNT(DISTINCT "customer_id") AS "uniq_customers"""")
  }

  // -- source reference variants --

  test("compile handles source without explicit catalog (engine-default)") {
    val source = SourceRef.ByName(
      catalog = None, namespace = None, table = "my_table",
    )
    val m = model(
      source = source,
      dimensions = List(Dimension.field("id", SealedDataType.BigInt)),
    )
    val sql = compiler.compile(m)
    sql shouldBe """SELECT "id" AS "id" FROM "my_table""""
  }

  test("compile handles source with only catalog and table (no namespace)") {
    val source = SourceRef.ByName(
      catalog = Some("hive"), namespace = None, table = "orders",
    )
    val m = model(
      source = source,
      dimensions = List(Dimension.field("id", SealedDataType.BigInt)),
    )
    val sql = compiler.compile(m)
    sql shouldBe """SELECT "id" AS "id" FROM "hive"."orders""""
  }

  // -- expression rendering --

  test("compile emits compound filter with AND") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      filters = List(
        FilterSpec("active", Expr.GreaterThan(
          Expr.FieldRef("amount"),
          Expr.Literal(LiteralValue.IntValue(0), SealedDataType.Int),
        )),
        FilterSpec("not_null", Expr.IsNotNull(Expr.FieldRef("customer_id"))),
      ),
    )
    val sql = compiler.compile(m)
    sql should include ("""WHERE (("amount" > 0)) AND (("customer_id" IS NOT NULL))""")
  }

  test("compile emits string literal with single-quote escaping") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      filters = List(FilterSpec("specific", Expr.Equal(
        Expr.FieldRef("carrier"),
        Expr.Literal(LiteralValue.StringValue("AA"), SealedDataType.Varchar),
      ))),
    )
    val sql = compiler.compile(m)
    sql should include ("""WHERE (("carrier" = 'AA'))""")
  }

  test("compile handles string literal with embedded single quote") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      filters = List(FilterSpec("quoted", Expr.Equal(
        Expr.FieldRef("region"),
        Expr.Literal(LiteralValue.StringValue("O'Brien"), SealedDataType.Varchar),
      ))),
    )
    val sql = compiler.compile(m)
    sql should include ("""WHERE (("region" = 'O''Brien'))""")
  }

  // -- aggregate function reference (varargs / unique coverage) --

  test("compile emits First/Last as FIRST_VALUE/LAST_VALUE") {
    val m = model(
      source = byName,
      dimensions = List(Dimension.field("region", SealedDataType.Varchar)),
      measures = List(
        Measure.aggregate("first_id", AggregateFn.First, Expr.FieldRef("id")),
        Measure.aggregate("last_id",  AggregateFn.Last,  Expr.FieldRef("id")),
      ),
    )
    val sql = compiler.compile(m)
    sql should include ("""FIRST_VALUE("id") AS "first_id"""")
    sql should include ("""LAST_VALUE("id") AS "last_id"""")
  }

  // -- source reference edge cases (should not reach here in practice) --

  test("compile surfaces error placeholder for ByPath sources (resolver normally rejects)") {
    val source = SourceRef.ByPath("parquet", "/data/orders", Map.empty)
    val m = model(
      source = source,
      dimensions = List(Dimension.field("id", SealedDataType.BigInt)),
    )
    val sql = compiler.compile(m)
    sql should include ("<error: path-based source not supported by Trino")
  }

  test("compile surfaces error placeholder for ByProvider sources (resolver normally rejects)") {
    val source = SourceRef.ByProvider(
      io.semanticdf.core.model.ProviderRef.DataFrameSource("myProvider", None),
    )
    val m = model(
      source = source,
      dimensions = List(Dimension.field("id", SealedDataType.BigInt)),
    )
    val sql = compiler.compile(m)
    sql should include ("<error: ProviderRef not supported by Trino")
  }

  // -- boundary contract --

  test("TrinoQueryCompiler is public") {
    val c = new TrinoQueryCompiler
    c shouldBe a [TrinoQueryCompiler]
  }
}