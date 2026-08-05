package io.semanticdf.trino

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.expr.LiteralValue

/** Phase 4: prove that the new `FakeTrinoConnection.catchAll`
  * factory returns the canned result for any SQL — independent of
  * the exact SQL string or parameter count.
  *
  * ==Why this exists==
  *
  * Per scala-data-driven-refactor §3: the catch-all is a NEW data
  * shape on the same field. The invariant — "any unmatched SQL
  * returns the catch-all result" — must be tested in isolation
  * from the existing `withResponse` path. If the invariant
  * breaks, every caller that uses `catchAll` quietly regresses.
  *
  * ==Why per-test verification (not a single round-trip test)==
  *
  * Catching mismatched behavior is more useful at the unit-level
  * than at the integration-level. Each test here exercises ONE
  * aspect of the catch-all semantics; together they pin down the
  * contract. */
class FakeTrinoConnectionCatchAllSpec extends AnyFunSuite with Matchers {

  private val fixedResult = TrinoResult(
    columns = List("a"),
    rows    = List(List(LiteralValue.IntValue(42))),
  )

  // -- happy path --

  test("catchAll returns the canned result regardless of SQL string") {
    val fakeConn = FakeTrinoConnection.catchAll(fixedResult)

    // SQL A
    val r1 = fakeConn.prepareStatement("SELECT 1", Nil)
    r1 shouldBe fixedResult

    // Totally different SQL B — still same result
    val r2 = fakeConn.prepareStatement(
      "SELECT * FROM anywhere.at.your.table WHERE x = ?",
      List(LiteralValue.IntValue(1)),
    )
    r2 shouldBe fixedResult
  }

  test("catchAll handles different parameter counts") {
    val fakeConn = FakeTrinoConnection.catchAll(fixedResult)

    fakeConn.prepareStatement("?", Nil)                  shouldBe fixedResult
    fakeConn.prepareStatement("?, ?, ?", List.fill(3)(LiteralValue.IntValue(0))) shouldBe fixedResult
    fakeConn.prepareStatement("no params here", Nil)    shouldBe fixedResult
  }

  // -- invariants --

  test("catchAll records every prepareStatement call") {
    val fakeConn = FakeTrinoConnection.catchAll(fixedResult)

    fakeConn.prepareStatement("SELECT 1", Nil)
    fakeConn.prepareStatement("SELECT 2", Nil)
    fakeConn.prepareStatement("SELECT 3", List(LiteralValue.StringValue("x")))

    fakeConn.recordedCalls.size shouldBe 3  // distinct (sql, params) keys
    fakeConn.recordedCalls.values.sum shouldBe 3  // 3 calls total
  }

  test("catchAll + responses: specific responses take precedence") {
    // When BOTH `responses` and `catchAll` are set, the specific
    // response wins (per `responses.get(key).orElse(catchAll)`).
    // The catch-all is only consulted when the specific lookup
    // misses.
    val specific = TrinoResult(
      columns = List("specific"),
      rows    = List(List(LiteralValue.StringValue("exact-match"))),
    )
    val generic = TrinoResult(
      columns = List("generic"),
      rows    = List(List(LiteralValue.StringValue("catch-all"))),
    )
    val fakeConn = FakeTrinoConnection(
      responses = Map(("SELECT 1", 0) -> specific),
      catchAll  = Some(generic),
    )

    // Exact match → specific
    fakeConn.prepareStatement("SELECT 1", Nil) shouldBe specific

    // No match → catch-all
    fakeConn.prepareStatement("SELECT 2", Nil) shouldBe generic
  }

  test("catchAll without responses: any SQL throws on missing catch-all setup") {
    // Sanity: a FakeTrinoConnection with NO responses and NO catch-all
    // still throws. This was the v1 behavior; it must remain the
    // baseline so `catchAll` is strictly an OPT-IN addition.
    val empty = FakeTrinoConnection()
    intercept[RuntimeException] {
      empty.prepareStatement("SELECT 1", Nil)
    }
  }

  // -- pattern-match coverage (scala-data-driven-refactor §3) --

  test("catchAll is closed-set: only `Some(TrinoResult)` or `None`") {
    // The catchAll field is `Option[TrinoResult]` (sealed), so
    // Scala's type system prevents nonsense values. This test
    // documents the closed-set invariant rather than enforcing
    // it (the type does that already).
    val fakeConn = FakeTrinoConnection.catchAll(fixedResult)
    fakeConn.catchAll shouldBe Some(fixedResult)
  }
}
