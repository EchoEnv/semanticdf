package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the `toErrorDetail` method on [EngineError]
  * (v0.3.0).
  *
  * The mapping is total over the `EngineError` ADT (the Scala
  * compiler enforces exhaustiveness via the `match`). Each case
  * produces a structured [ErrorDetail] with:
  *   - `code`: a stable uppercase token drawn from the closed list
  *   - `message`: a human-readable, engine-aware string
  *   - `hint`: an actionable fix (None when no action is suggested)
  *   - `details`: the case fields, keyed by name */
class EngineErrorToErrorDetailSpec extends AnyFunSuite with Matchers {

  // -- UnsupportedCapability --

  test("UnsupportedCapability maps to UNSUPPORTED_CAPABILITY") {
    val detail = EngineError.UnsupportedCapability(
      name   = "broadcast-joins",
      reason = "session disabled",
    ).toErrorDetail
    detail.code shouldBe "UNSUPPORTED_CAPABILITY"
    detail.message should include ("broadcast-joins")
    detail.message should include ("session disabled")
    detail.hint shouldBe defined
    detail.details.keySet should contain ("capability")
    detail.details("capability") shouldBe "broadcast-joins"
  }

  // -- IncompatibleExprShape --

  test("IncompatibleExprShape maps to INCOMPATIBLE_EXPR_SHAPE") {
    val detail = EngineError.IncompatibleExprShape(
      shape  = "nested-struct-literal",
      engine = "trino-432",
    ).toErrorDetail
    detail.code shouldBe "INCOMPATIBLE_EXPR_SHAPE"
    detail.message should include ("nested-struct-literal")
    detail.message should include ("trino-432")
    detail.details("shape") shouldBe "nested-struct-literal"
    detail.details("engine") shouldBe "trino-432"
  }

  // -- DecimalOverflow --

  test("DecimalOverflow maps to DECIMAL_OVERFLOW") {
    val detail = EngineError.DecimalOverflow(
      value     = "1234567890.123",
      precision = 10,
      scale     = 2,
    ).toErrorDetail
    detail.code shouldBe "DECIMAL_OVERFLOW"
    detail.message should include ("1234567890.123")
    detail.message should include ("DECIMAL(10, 2)")
    detail.details("value") shouldBe "1234567890.123"
    detail.details("precision") shouldBe "10"
    detail.details("scale") shouldBe "2"
  }

  // -- FeatureDeferred --

  test("FeatureDeferred maps to FEATURE_DEFERRED") {
    val detail = EngineError.FeatureDeferred(
      feature = "window-functions",
      release = "v0.4.0",
    ).toErrorDetail
    detail.code shouldBe "FEATURE_DEFERRED"
    detail.message should include ("window-functions")
    detail.message should include ("v0.4.0")
    detail.details("feature") shouldBe "window-functions"
    detail.details("release") shouldBe "v0.4.0"
  }

  // -- CancellationFailed --

  test("CancellationFailed maps to CANCELLATION_FAILED") {
    val detail = EngineError.CancellationFailed(
      cancelStatus = "still-running",
    ).toErrorDetail
    detail.code shouldBe "CANCELLATION_FAILED"
    detail.message should include ("still-running")
    detail.details("cancel_status") shouldBe "still-running"
  }

  // -- ConnectionFailed --

  test("ConnectionFailed maps to CONNECTION_FAILED") {
    val detail = EngineError.ConnectionFailed(
      reason = "ECONNREFUSED 10.0.0.1:8080",
    ).toErrorDetail
    detail.code shouldBe "CONNECTION_FAILED"
    detail.message should include ("ECONNREFUSED 10.0.0.1:8080")
    detail.details("reason") shouldBe "ECONNREFUSED 10.0.0.1:8080"
  }

  // -- QueryTimedOut --

  test("QueryTimedOut maps to QUERY_TIMED_OUT") {
    val detail = EngineError.QueryTimedOut(
      cancelStatus = "aborted-mid-pipeline",
    ).toErrorDetail
    detail.code shouldBe "QUERY_TIMED_OUT"
    detail.message should include ("aborted-mid-pipeline")
    detail.details("cancel_status") shouldBe "aborted-mid-pipeline"
  }

  // -- AuditSinkUnavailable --

  test("AuditSinkUnavailable maps to AUDIT_SINK_UNAVAILABLE") {
    val detail = EngineError.AuditSinkUnavailable(
      name = "kafka-prod",
    ).toErrorDetail
    detail.code shouldBe "AUDIT_SINK_UNAVAILABLE"
    detail.message should include ("kafka-prod")
    detail.details("sink_name") shouldBe "kafka-prod"
  }

  // -- ProviderInvocationFailed --

  test("ProviderInvocationFailed maps to PROVIDER_INVOCATION_FAILED") {
    val detail = EngineError.ProviderInvocationFailed(
      name   = "my-provider",
      reason = "S3 access denied",
    ).toErrorDetail
    detail.code shouldBe "PROVIDER_INVOCATION_FAILED"
    detail.message should include ("my-provider")
    detail.message should include ("S3 access denied")
    detail.details("provider") shouldBe "my-provider"
    detail.details("reason") shouldBe "S3 access denied"
  }

  // -- SourceSchemaChanged --

  test("SourceSchemaChanged maps to SOURCE_SCHEMA_CHANGED") {
    val detail = EngineError.SourceSchemaChanged(
      source = "orders_v3",
    ).toErrorDetail
    detail.code shouldBe "SOURCE_SCHEMA_CHANGED"
    detail.message should include ("orders_v3")
    detail.details("source") shouldBe "orders_v3"
  }

  // -- EngineUnavailable --

  test("EngineUnavailable maps to ENGINE_UNAVAILABLE (wasDefault = false)") {
    val detail = EngineError.EngineUnavailable(
      name       = "trino",
      available  = Seq("spark", "duckdb"),
      wasDefault = false,
    ).toErrorDetail
    detail.code shouldBe "ENGINE_UNAVAILABLE"
    detail.message should include ("'trino' is not registered")
    detail.message should include ("spark")
    detail.message should include ("duckdb")
    detail.details("engine") shouldBe "trino"
    detail.details("available") shouldBe "spark,duckdb"
    detail.details("was_default") shouldBe "false"
  }

  test("EngineUnavailable maps to ENGINE_UNAVAILABLE (wasDefault = true)") {
    val detail = EngineError.EngineUnavailable(
      name       = "trino",
      available  = Seq("spark"),
      wasDefault = true,
    ).toErrorDetail
    detail.code shouldBe "ENGINE_UNAVAILABLE"
    detail.message should include ("Default engine 'trino' is unavailable")
    detail.details("was_default") shouldBe "true"
  }

  // -- Cross-cutting --

  test("toErrorDetail returns a non-null ErrorDetail for every case") {
    val samples: Seq[EngineError] = Seq(
      EngineError.UnsupportedCapability("x", "y"),
      EngineError.IncompatibleExprShape("x", "y"),
      EngineError.DecimalOverflow("x", 1, 1),
      EngineError.FeatureDeferred("x", "y"),
      EngineError.CancellationFailed("x"),
      EngineError.ConnectionFailed("x"),
      EngineError.QueryTimedOut("x"),
      EngineError.AuditSinkUnavailable("x"),
      EngineError.ProviderInvocationFailed("x", "y"),
      EngineError.SourceSchemaChanged("x"),
      EngineError.EngineUnavailable("x", Seq.empty, wasDefault = false),
    )
    samples.foreach { e =>
      val d = e.toErrorDetail
      d.code should not be empty
      d.message should not be empty
    }
  }

  test("every code is unique across cases") {
    val codes: Seq[String] = Seq(
      EngineError.UnsupportedCapability("x", "y"),
      EngineError.IncompatibleExprShape("x", "y"),
      EngineError.DecimalOverflow("x", 1, 1),
      EngineError.FeatureDeferred("x", "y"),
      EngineError.CancellationFailed("x"),
      EngineError.ConnectionFailed("x"),
      EngineError.QueryTimedOut("x"),
      EngineError.AuditSinkUnavailable("x"),
      EngineError.ProviderInvocationFailed("x", "y"),
      EngineError.SourceSchemaChanged("x"),
      EngineError.EngineUnavailable("x", Seq.empty, wasDefault = false),
    ).map(_.toErrorDetail.code)
    codes.distinct.size shouldBe codes.size
  }
}