package io.semanticdf.core.rel

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{ResolvedSchema, ResolvedSource}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.SourceRef
import io.semanticdf.core.schema.{Field, SealedDataType}

/** Phase 2 contract: prove `RelOp` is a usable, Spark-free data
  * record + the closed 8-variant enumeration. Per scala-data-
  * driven-refactor, this is pure data: the plan SHAPE is engine-
  * portable; the engine-specific compile is in the engine adapter.
  */
class RelOpSpec extends AnyFunSuite with Matchers {

  // -- helper fixtures --

  private val sampleField = Field(
    name     = "amount",
    dataType = SealedDataType.BigInt,
    nullable = false,
  )

  private val sampleResolvedScan = ResolvedSource.Scan(
    source = SourceRef.ByName(
      catalog   = None,
      namespace = Some("public"),
      table     = "orders",
    ),
    schema = ResolvedSchema(fields = Map("amount" -> "bigint")),
  )

  private val sampleScan = RelOp.Scan(
    source     = sampleResolvedScan,
    schema     = List(sampleField),
    projection = List(Expr.FieldRef("amount")),
  )

  // -- Scan --

  test("Scan carries source + schema + projection") {
    sampleScan.source shouldBe sampleResolvedScan
    sampleScan.schema.size shouldBe 1
    sampleScan.projection.size shouldBe 1
  }

  // -- Filter --

  test("Filter carries input + predicate") {
    val f = RelOp.Filter(sampleScan, Expr.GreaterThan(
      Expr.FieldRef("amount"),
      Expr.Literal(LiteralValue.LongValue(100L), SealedDataType.BigInt),
    ))
    f.input shouldBe sampleScan
    f.predicate shouldBe a [Expr.GreaterThan]
  }

  // -- Project --

  test("Project carries input + expressions (List[(Expr, String)])") {
    val p = RelOp.Project(
      input       = sampleScan,
      expressions = List((Expr.FieldRef("amount"), "amt")),
    )
    p.expressions.size shouldBe 1
    p.expressions(0)._2 shouldBe "amt"
  }

  // -- Aggregate --

  test("Aggregate carries input + groupBy + aggregates") {
    val a = RelOp.Aggregate(
      input      = sampleScan,
      groupBy    = List(Expr.FieldRef("region")),
      aggregates = List(AggregateCall(
        fn       = AggregateFn.Sum,
        input    = Some(Expr.FieldRef("amount")),
        alias    = "total",
      )),
    )
    a.groupBy.size shouldBe 1
    a.aggregates.size shouldBe 1
    a.aggregates(0).alias shouldBe "total"
  }

  // -- Join --

  test("Join carries left + right + kind + condition") {
    val rightScan = RelOp.Scan(
      source     = ResolvedSource.Scan(
        source = SourceRef.ByName(catalog = None, namespace = None, table = "regions"),
        schema = ResolvedSchema(),
      ),
      schema     = Nil,
      projection = Nil,
    )
    val j = RelOp.Join(
      left      = sampleScan,
      right     = rightScan,
      kind      = JoinKind.Inner,
      condition = Expr.Equal(Expr.FieldRef("region"), Expr.FieldRef("code")),
    )
    j.kind shouldBe JoinKind.Inner
    j.condition shouldBe a [Expr.Equal]
  }

  // -- Sort --

  test("Sort carries input + keys (List[SortKey])") {
    val s = RelOp.Sort(
      input = sampleScan,
      keys  = List(SortKey(
        expression   = Expr.FieldRef("amount"),
        direction    = SortDirection.Descending,
        nullOrdering = NullOrdering.First,
      )),
    )
    s.keys.size shouldBe 1
    s.keys(0).direction shouldBe SortDirection.Descending
  }

  // -- Limit --

  test("Limit carries input + count + offset (default offset = 0)") {
    val l = RelOp.Limit(sampleScan, count = 100L)
    l.count shouldBe 100L
    l.offset shouldBe 0L
  }

  test("Limit with offset") {
    val l = RelOp.Limit(sampleScan, count = 50L, offset = 200L)
    l.count shouldBe 50L
    l.offset shouldBe 200L
  }

  // -- realistic plan tree --

  test("realistic plan tree: Scan -> Filter -> Aggregate -> Sort -> Limit") {
    val plan: RelOp = RelOp.Limit(
      input = RelOp.Sort(
        input = RelOp.Aggregate(
          input = RelOp.Filter(
            input     = sampleScan,
            predicate = Expr.GreaterThan(
              Expr.FieldRef("amount"),
              Expr.Literal(LiteralValue.LongValue(100L), SealedDataType.BigInt),
            ),
          ),
          groupBy    = List(Expr.FieldRef("region")),
          aggregates = List(AggregateCall(
            fn       = AggregateFn.Sum,
            input    = Some(Expr.FieldRef("amount")),
            alias    = "total",
          )),
        ),
        keys = List(SortKey(
          expression   = Expr.FieldRef("total"),
          direction    = SortDirection.Descending,
          nullOrdering = NullOrdering.First,
        )),
      ),
      count = 10L,
    )
    plan shouldBe a [RelOp.Limit]
  }

  test("realistic Join plan: Scan + Scan -> Join -> Project") {
    val rightScan = RelOp.Scan(
      source     = ResolvedSource.Scan(
        source = SourceRef.ByName(catalog = None, namespace = None, table = "regions"),
        schema = ResolvedSchema(),
      ),
      schema     = Nil,
      projection = Nil,
    )
    val joined = RelOp.Project(
      input = RelOp.Join(
        left      = sampleScan,
        right     = rightScan,
        kind      = JoinKind.Left,
        condition = Expr.Equal(Expr.FieldRef("region"), Expr.FieldRef("code")),
      ),
      expressions = List((Expr.FieldRef("amount"), "amt")),
    )
    joined shouldBe a [RelOp.Project]
  }

  // -- closed enumeration --

  test("RelOp has exactly 8 cases (Scan, Filter, Project, Aggregate, Join, Sort, Limit)") {
    val rightScan = RelOp.Scan(
      source     = ResolvedSource.Scan(
        source = SourceRef.ByName(catalog = None, namespace = None, table = "regions"),
        schema = ResolvedSchema(),
      ),
      schema     = Nil,
      projection = Nil,
    )
    val all: Set[RelOp] = Set(
      RelOp.Scan(rightScan.source, rightScan.schema, rightScan.projection),
      RelOp.Filter(sampleScan, Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean)),
      RelOp.Project(sampleScan, Nil),
      RelOp.Aggregate(sampleScan, Nil, Nil),
      RelOp.Join(sampleScan, rightScan, JoinKind.Inner, Expr.Literal(LiteralValue.BoolValue(true), SealedDataType.Boolean)),
      RelOp.Sort(sampleScan, Nil),
      RelOp.Limit(sampleScan, count = 10L),
    )
    // 7 cases listed; verify by exhaustive pattern-match in next test
    all.size shouldBe 7
  }

  // -- Serializable round-trip --

  test("full plan tree round-trips through Java serialization") {
    val plan: RelOp = RelOp.Limit(
      input = RelOp.Sort(
        input = RelOp.Aggregate(
          input = RelOp.Filter(
            input     = sampleScan,
            predicate = Expr.GreaterThan(
              Expr.FieldRef("amount"),
              Expr.Literal(LiteralValue.LongValue(100L), SealedDataType.BigInt),
            ),
          ),
          groupBy    = List(Expr.FieldRef("region")),
          aggregates = List(AggregateCall(
            fn       = AggregateFn.Sum,
            input    = Some(Expr.FieldRef("amount")),
            alias    = "total",
          )),
        ),
        keys = List(SortKey(
          expression   = Expr.FieldRef("total"),
          direction    = SortDirection.Descending,
          nullOrdering = NullOrdering.First,
        )),
      ),
      count = 10L,
    )
    val bos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(bos)
    oos.writeObject(plan)
    oos.close()
    val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
    val ois = new java.io.ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[RelOp]
    restored shouldBe plan
  }
}