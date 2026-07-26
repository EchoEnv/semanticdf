package io.semanticdf.lineage

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Unit tests for [[ColumnRefExtractor]] — the SQL-to-column-refs
  * parser that drives the lineage. */
class ColumnRefExtractorSpec extends AnyFunSuite {

  test("extract: bare column name") {
    assert(ColumnRefExtractor.extract("carrier") == Seq("carrier"))
  }

  test("extract: qualified column name (case-preserved)") {
    assert(ColumnRefExtractor.extract("Customers.OrderDate") == Seq("Customers.OrderDate"))
  }

  test("extract: function call with a column argument") {
    assert(ColumnRefExtractor.extract("upper(carrier)") == Seq("carrier"))
  }

  test("extract: function call with a qualified column argument") {
    assert(ColumnRefExtractor.extract("upper(Customers.Name)") == Seq("Customers.Name"))
  }

  test("extract: multiple column refs in source order") {
    assert(ColumnRefExtractor.extract("case when carrier in ('AA','UA') then distance else 0 end")
      == Seq("carrier", "distance"))
  }

  test("extract: deeply qualified name (multi-part)") {
    // 3-part qualifier: schema.table.column — preserved as a single
    // dotted string in the lineage.
    assert(ColumnRefExtractor.extract("warehouse.public.orders.amount")
      == Seq("warehouse.public.orders.amount"))
  }

  test("extract: constant expression returns empty") {
    assert(ColumnRefExtractor.extract("42") == Seq.empty)
    assert(ColumnRefExtractor.extract("'hello'") == Seq.empty)
    assert(ColumnRefExtractor.extract("true") == Seq.empty)
  }

  test("extract: arithmetic with two columns") {
    assert(ColumnRefExtractor.extract("revenue - cost") == Seq("revenue", "cost"))
  }

  test("extract: invalid SQL returns empty (does not throw)") {
    // The extractor is best-effort: invalid SQL = empty list. The
    // caller (the lineage builder) translates this to LineageStatus.Partial.
    assert(ColumnRefExtractor.extract("totally invalid !@# sql here") == Seq.empty)
  }

  test("extract: empty string returns empty") {
    assert(ColumnRefExtractor.extract("") == Seq.empty)
  }
}
