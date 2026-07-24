package io.semanticdf

import org.apache.spark.sql.Column
import org.scalatest.funspec.AnyFunSpec

/** Round-trip + construction tests for the structured predicate AST
  * (v0.1.13). This closes the last remaining narrow caveat of the
  * joined-models-manifest recipe: non-equi / OR predicates used to
  * round-trip as an opaque `onExprString` SQL field. The AST gives
  * tools a structured view of the predicate. */
class PredicateAstSpec extends AnyFunSpec {

  describe("PredicateAst.Op") {
    it("has stable wire codes") {
      assert(PredicateAst.Op.Eq.code  == "eq")
      assert(PredicateAst.Op.Neq.code == "neq")
      assert(PredicateAst.Op.Lt.code  == "lt")
      assert(PredicateAst.Op.Lte.code == "lte")
      assert(PredicateAst.Op.Gt.code  == "gt")
      assert(PredicateAst.Op.Gte.code == "gte")
      assert(PredicateAst.Op.And.code == "and")
      assert(PredicateAst.Op.Or.code  == "or")
    }

    it("round-trips through fromCode") {
      for (op <- Seq(PredicateAst.Op.Eq, PredicateAst.Op.Neq, PredicateAst.Op.Lt,
                     PredicateAst.Op.Lte, PredicateAst.Op.Gt, PredicateAst.Op.Gte,
                     PredicateAst.Op.And, PredicateAst.Op.Or))
        assert(PredicateAst.Op.fromCode(op.code).contains(op))
    }

    it("fromCode returns None for unknown codes") {
      assert(PredicateAst.Op.fromCode("bogus").isEmpty)
      assert(PredicateAst.Op.fromCode("EQ").isEmpty)  // case-sensitive
    }

    it("uses reference equality (O(1) compare)") {
      assert(PredicateAst.Op.Eq == PredicateAst.Op.Eq)
      assert(PredicateAst.Op.Eq != PredicateAst.Op.Neq)
      assert(PredicateAst.Op.Eq.hashCode == System.identityHashCode(PredicateAst.Op.Eq))
    }
  }

  describe("PredicateAst.Operand.ColumnRef") {
    it("preserves side and name") {
      val cr = PredicateAst.Operand.ColumnRef("left", "k1")
      assert(cr.side == "left")
      assert(cr.name == "k1")
    }
  }

  describe("PredicateAst.Predicate.toColumn") {
    it("builds an equi-join column") {
      val ast = PredicateAst.Predicate(
        op = PredicateAst.Op.Eq,
        left = PredicateAst.Operand.ColumnRef("left", "id"),
        right = PredicateAst.Operand.ColumnRef("right", "id"),
      )
      val spark = SemanticTableFixture.spark
      import spark.implicits._
      val ldf = Seq((1, "2024-01-01", "x")).toDF("id", "date", "name")
      val rdf = Seq((1, "2024-01-02", "y")).toDF("id", "date", "name")
      val leftSide  = new JoinSide("left",  ldf, Map.empty, scala.collection.mutable.Map.empty)
      val rightSide = new JoinSide("right", rdf, Map.empty, scala.collection.mutable.Map.empty)
      val col: Column = ast.toColumn(leftSide, rightSide)
      assert(col != null)
      // Same scopes -> cache hit, same Column instance.
      val col2: Column = ast.toColumn(leftSide, rightSide)
      assert(col eq col2)
    }

    it("caches per (leftSide, rightSide) pair") {
      val ast = PredicateAst.Predicate(
        op = PredicateAst.Op.Lt,
        left = PredicateAst.Operand.ColumnRef("left", "date"),
        right = PredicateAst.Operand.ColumnRef("right", "date"),
      )
      val ldf1 = SemanticTableFixture.tinyDf("id1", 1)
      val rdf1 = SemanticTableFixture.tinyDf("id1", 2)
      val ldf2 = SemanticTableFixture.tinyDf("id2", 1)
      val rdf2 = SemanticTableFixture.tinyDf("id2", 2)
      val left1  = new JoinSide("left",  ldf1, Map.empty, scala.collection.mutable.Map.empty)
      val right1 = new JoinSide("right", rdf1, Map.empty, scala.collection.mutable.Map.empty)
      val left2  = new JoinSide("left",  ldf2, Map.empty, scala.collection.mutable.Map.empty)
      val right2 = new JoinSide("right", rdf2, Map.empty, scala.collection.mutable.Map.empty)
      val c1 = ast.toColumn(left1, right1)
      val c2 = ast.toColumn(left2, right2)
      // Different scopes -> different columns.
      assert(c1 ne c2)
      // But the same scopes reuse the cached one.
      val c1again = ast.toColumn(left1, right1)
      assert(c1 eq c1again)
    }

    it("supports all operators without throwing") {
      val ops = Seq(PredicateAst.Op.Eq, PredicateAst.Op.Neq, PredicateAst.Op.Lt,
                    PredicateAst.Op.Lte, PredicateAst.Op.Gt, PredicateAst.Op.Gte,
                    PredicateAst.Op.And, PredicateAst.Op.Or)
      val ldf = SemanticTableFixture.tinyDf("a", 1)
      val rdf = SemanticTableFixture.tinyDf("a", 2)
      val l = new JoinSide("left",  ldf, Map.empty, scala.collection.mutable.Map.empty)
      val r = new JoinSide("right", rdf, Map.empty, scala.collection.mutable.Map.empty)
      for (op <- ops) {
        val p = PredicateAst.Predicate(
          op = op,
          left = PredicateAst.Operand.ColumnRef("left", "id"),
          right = PredicateAst.Operand.ColumnRef("right", "date"),
        )
        val c: Column = p.toColumn(l, r)
        assert(c != null)
      }
    }

    it("supports nested predicates (and/or)") {
      // (left.x === right.x) AND (left.y < right.y)
      val ldf = SemanticTableFixture.tinyDf("x", 1)
      val rdf = SemanticTableFixture.tinyDf("x", 2)
      val leftSide  = new JoinSide("left",  ldf, Map.empty, scala.collection.mutable.Map.empty)
      val rightSide = new JoinSide("right", rdf, Map.empty, scala.collection.mutable.Map.empty)
      val equi = PredicateAst.Predicate(
        op = PredicateAst.Op.Eq,
        left = PredicateAst.Operand.ColumnRef("left", "id"),
        right = PredicateAst.Operand.ColumnRef("right", "id"),
      )
      val range_ = PredicateAst.Predicate(
        op = PredicateAst.Op.Lt,
        left = PredicateAst.Operand.ColumnRef("left", "date"),
        right = PredicateAst.Operand.ColumnRef("right", "date"),
      )
      val andPred = PredicateAst.Predicate(
        op = PredicateAst.Op.And,
        left = equi,
        right = range_,
      )
      val col: Column = andPred.toColumn(leftSide, rightSide)
      assert(col != null)
    }
  }

  describe("PredicateAst extraction from SemanticJoinOp") {
    it("extracts an equi-join AST") {
      val leftTable = SemanticTableFixture.simple(2, "left")
      val rightTable = SemanticTableFixture.simple(2, "right")
      val joined = leftTable.join_one(rightTable, { (l, r) => l("id") === r("id") })
      // Equi-join: keys alone are sufficient. The AST is still
      // populated (the writer chooses whether to emit it). For
      // v0.1.13 we always populate it so the wire shape is uniform.
      val join = joined.root.asInstanceOf[SemanticJoinOp]
      assert(join.predicateAst.isDefined)
      val ast = join.predicateAst.get
      assert(ast.op == PredicateAst.Op.Eq)
    }

    it("extracts a non-equi AST (lt)") {
      val leftTable  = SemanticTableFixture.simple(2, "left")
      val rightTable = SemanticTableFixture.simple(2, "right")
      val joined = leftTable.join_one(rightTable, { (l, r) => l("date") < r("date") })
      val join = joined.root.asInstanceOf[SemanticJoinOp]
      assert(join.predicateAst.isDefined)
      assert(join.predicateAst.get.op == PredicateAst.Op.Lt)
    }

    it("extracts a compound AND AST") {
      val leftTable  = SemanticTableFixture.simple(2, "left")
      val rightTable = SemanticTableFixture.simple(2, "right")
      val joined = leftTable.join_one(rightTable, { (l, r) =>
        l("id") === r("id") && l("date") < r("date")
      })
      val join = joined.root.asInstanceOf[SemanticJoinOp]
      assert(join.predicateAst.isDefined)
      assert(join.predicateAst.get.op == PredicateAst.Op.And)
    }

    it("extracts a compound OR AST") {
      val leftTable  = SemanticTableFixture.simple(2, "left")
      val rightTable = SemanticTableFixture.simple(2, "right")
      val joined = leftTable.join_one(rightTable, { (l, r) =>
        l("a") === r("a") || l("b") === r("b")
      })
      val join = joined.root.asInstanceOf[SemanticJoinOp]
      assert(join.predicateAst.isDefined)
      assert(join.predicateAst.get.op == PredicateAst.Op.Or)
    }

    it("the equi-join case still keeps zero AST keys-only path viable") {
      // The keys field stays the primary representation. The AST is
      // a parallel structure for tools that want a structural view.
      val leftTable  = SemanticTableFixture.simple(2, "left")
      val rightTable = SemanticTableFixture.simple(2, "right")
      val joined = leftTable.join_one(rightTable, { (l, r) => l("id") === r("id") })
      val join = joined.root.asInstanceOf[SemanticJoinOp]
      assert(join.leftKeys == Seq("id"))
      assert(join.rightKeys == Seq("id"))
      assert(join.predicateAst.isDefined)
    }
  }
}
