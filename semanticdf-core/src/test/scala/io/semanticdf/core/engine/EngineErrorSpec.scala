package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove the `EngineError` sealed ADT is a usable,
  * Spark-free data record. Every case class carries exactly the data
  * needed to identify the failure mode. Per scala-data-driven-refactor,
  * `EngineError` is pure data — no behavior, no engine coupling
  * beyond the `engine` field on `IncompatibleExprShape`.
  *
  * ==Why a sealed ADT==
  *
  * The MCP server's error mapping is exhaustive over the ADT — adding
  * a new case requires updating the mapper. This forces engine
  * adapters to use the closed set of failure modes, not arbitrary
  * exceptions with free-form messages.
  */
class EngineErrorSpec extends AnyFunSuite with Matchers {

  // -- case-by-case construction + equality --

  test("UnsupportedCapability carries name and reason") {
    val e = EngineError.UnsupportedCapability("window-ranking", "no window support")
    e.name shouldBe "window-ranking"
    e.reason shouldBe "no window support"
  }

  test("IncompatibleExprShape carries shape and engine") {
    val e = EngineError.IncompatibleExprShape("nested struct", "sqlite")
    e.shape shouldBe "nested struct"
    e.engine shouldBe "sqlite"
  }

  test("DecimalOverflow carries value, precision, scale") {
    val e = EngineError.DecimalOverflow("123.456", 10, 2)
    e.value shouldBe "123.456"
    e.precision shouldBe 10
    e.scale shouldBe 2
  }

  test("FeatureDeferred carries feature and release") {
    val e = EngineError.FeatureDeferred("broadcast-join", "v0.4.0")
    e.feature shouldBe "broadcast-join"
    e.release shouldBe "v0.4.0"
  }

  test("CancellationFailed carries cancelStatus") {
    val e = EngineError.CancellationFailed("still running")
    e.cancelStatus shouldBe "still running"
  }

  test("ConnectionFailed carries reason") {
    val e = EngineError.ConnectionFailed("ECONNREFUSED")
    e.reason shouldBe "ECONNREFUSED"
  }

  test("QueryTimedOut carries cancelStatus") {
    val e = EngineError.QueryTimedOut("aborted")
    e.cancelStatus shouldBe "aborted"
  }

  test("AuditSinkUnavailable carries name") {
    val e = EngineError.AuditSinkUnavailable("postgres-sink")
    e.name shouldBe "postgres-sink"
  }

  test("ProviderInvocationFailed carries name and reason") {
    val e = EngineError.ProviderInvocationFailed("csv-source", "file not found")
    e.name shouldBe "csv-source"
    e.reason shouldBe "file not found"
  }

  test("SourceSchemaChanged carries source") {
    val e = EngineError.SourceSchemaChanged("orders.csv")
    e.source shouldBe "orders.csv"
  }

  test("EngineUnavailable carries name, available list, wasDefault") {
    val e = EngineError.EngineUnavailable("trino", Seq("spark", "databricks"), wasDefault = true)
    e.name shouldBe "trino"
    e.available shouldBe Seq("spark", "databricks")
    e.wasDefault shouldBe true
  }

  // -- sealed exhaustiveness + Product with Serializable --

  test("Sealed exhaustiveness: pattern-match over all 11 cases") {
    val examples: Seq[EngineError] = Seq(
      EngineError.UnsupportedCapability("a", "b"),
      EngineError.IncompatibleExprShape("a", "b"),
      EngineError.DecimalOverflow("a", 1, 2),
      EngineError.FeatureDeferred("a", "b"),
      EngineError.CancellationFailed("a"),
      EngineError.ConnectionFailed("a"),
      EngineError.QueryTimedOut("a"),
      EngineError.AuditSinkUnavailable("a"),
      EngineError.ProviderInvocationFailed("a", "b"),
      EngineError.SourceSchemaChanged("a"),
      EngineError.EngineUnavailable("a", Seq.empty, wasDefault = false),
    )
    examples.foreach {
      case EngineError.UnsupportedCapability(_, _)      => ()
      case EngineError.IncompatibleExprShape(_, _)      => ()
      case EngineError.DecimalOverflow(_, _, _)          => ()
      case EngineError.FeatureDeferred(_, _)            => ()
      case EngineError.CancellationFailed(_)            => ()
      case EngineError.ConnectionFailed(_)              => ()
      case EngineError.QueryTimedOut(_)                 => ()
      case EngineError.AuditSinkUnavailable(_)         => ()
      case EngineError.ProviderInvocationFailed(_, _)  => ()
      case EngineError.SourceSchemaChanged(_)          => ()
      case EngineError.EngineUnavailable(_, _, _)       => ()
    }
  }

  test("Equality: same data => equal") {
    EngineError.UnsupportedCapability("x", "y") shouldBe
      EngineError.UnsupportedCapability("x", "y")
    EngineError.DecimalOverflow("1.0", 5, 2) shouldBe
      EngineError.DecimalOverflow("1.0", 5, 2)
  }

  test("Different case => not equal even with same fields") {
    val a = EngineError.UnsupportedCapability("x", "y")
    val b = EngineError.FeatureDeferred("x", "y")
    a should not be b
  }
}