package io.semanticdf.tools

import io.semanticdf.Predicate

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Unit tests for the SQL → SemanticTable.query adapter.
  *
  * Goal: pin the parser grammar. The runtime execution path is covered
  * by the existing SemanticTable spec — we only verify here that SQL
  * strings map to the right query() parameters. */
class SqlCliSpec extends AnyFunSuite with Matchers {

  test("parses a minimal SELECT ... FROM") {
    val p = SqlCli.parse("SELECT carrier FROM flights")
    assert(p.model == "flights")
    assert(p.selItems == Seq(SqlCli.SelItem("carrier", None)))
    assert(p.where.isEmpty)
    assert(p.orderBy.isEmpty)
    assert(p.limit.isEmpty)
  }

  test("parses SELECT *") {
    val p = SqlCli.parse("SELECT * FROM flights")
    assert(p.selItems == Seq(SqlCli.SelItem("*", None)))
  }

  test("parses multiple SELECT items with AS aliases") {
    val p = SqlCli.parse("SELECT carrier AS c, total_revenue AS rev FROM flights")
    assert(p.selItems == Seq(
      SqlCli.SelItem("carrier", Some("c")),
      SqlCli.SelItem("total_revenue", Some("rev")),
    ))
  }

  test("parses WHERE with a single comparison") {
    val p = SqlCli.parse("SELECT carrier FROM flights WHERE total_passengers > 100")
    assert(p.where.contains(Predicate.Compare("gt", "total_passengers", 100L)))
  }

  test("parses WHERE with string value") {
    val p = SqlCli.parse("SELECT carrier FROM flights WHERE carrier = 'AA'")
    assert(p.where.contains(Predicate.Compare("eq", "carrier", "AA")))
  }

  test("parses WHERE with AND chain") {
    val p = SqlCli.parse("SELECT carrier FROM flights WHERE carrier = 'AA' AND total_passengers > 100")
    val pred = p.where.get
    assert(pred.isInstanceOf[Predicate.And])
    val children = pred.asInstanceOf[Predicate.And].children
    assert(children.length == 2)
    assert(children(0).isInstanceOf[Predicate.Compare])
    assert(children(1).isInstanceOf[Predicate.Compare])
  }

  test("parses WHERE with OR chain") {
    val p = SqlCli.parse("SELECT carrier FROM flights WHERE carrier = 'AA' OR carrier = 'UA'")
    assert(p.where.exists(_.isInstanceOf[Predicate.Or]))
  }

  test("parses ORDER BY with ASC") {
    val p = SqlCli.parse("SELECT carrier FROM flights ORDER BY carrier ASC")
    assert(p.orderBy.length == 1)
    assert(p.orderBy.head.isInstanceOf[io.semanticdf.SortKey.Asc])
  }

  test("parses ORDER BY with DESC") {
    val p = SqlCli.parse("SELECT carrier FROM flights ORDER BY total_revenue DESC")
    assert(p.orderBy.length == 1)
    assert(p.orderBy.head.isInstanceOf[io.semanticdf.SortKey.Desc])
  }

  test("parses LIMIT") {
    val p = SqlCli.parse("SELECT carrier FROM flights LIMIT 10")
    assert(p.limit == Some(10))
  }

  test("accepts and ignores GROUP BY in any position") {
    val p1 = SqlCli.parse("SELECT carrier FROM flights GROUP BY carrier")
    assert(p1.selItems == Seq(SqlCli.SelItem("carrier", None)))
    val p2 = SqlCli.parse("SELECT carrier FROM flights WHERE carrier = 'AA' GROUP BY carrier")
    assert(p2.where.isDefined)
    val p3 = SqlCli.parse("SELECT carrier FROM flights ORDER BY carrier GROUP BY carrier")
    assert(p3.orderBy.length == 1)
  }

  test("parses full query") {
    val p = SqlCli.parse(
      "SELECT carrier, total_revenue " +
      "FROM flights " +
      "WHERE carrier = 'AA' " +
      "ORDER BY total_revenue DESC " +
      "LIMIT 5"
    )
    assert(p.model == "flights")
    assert(p.selItems.length == 2)
    assert(p.where.isDefined)
    assert(p.orderBy.length == 1)
    assert(p.orderBy.head.isInstanceOf[io.semanticdf.SortKey.Desc])
    assert(p.limit == Some(5))
  }

  test("rejects unknown clause keyword") {
    val ex = intercept[IllegalArgumentException] {
      SqlCli.parse("SELECT a FROM b FROBNICATE c")
    }
    assert(ex.getMessage.contains("FROBNICATE"))
  }

  test("rejects missing FROM clause") {
    val ex = intercept[IllegalArgumentException] {
      SqlCli.parse("SELECT a")
    }
    assert(ex.getMessage.contains("FROM"))
  }

  test("rejects unbalanced parentheses") {
    val ex = intercept[IllegalArgumentException] {
      SqlCli.parse("SELECT a FROM b WHERE (a = 1 AND c = 2")
    }
    assert(ex.getMessage.contains(")") || ex.getMessage.contains("expected"))
  }

  test("rejects trailing garbage") {
    val ex = intercept[IllegalArgumentException] {
      SqlCli.parse("SELECT a FROM b unexpected")
    }
    assert(ex.getMessage.toLowerCase.contains("unexpected"))
  }
}
