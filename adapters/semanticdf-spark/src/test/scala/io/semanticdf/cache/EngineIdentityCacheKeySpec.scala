package io.semanticdf.cache

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.EngineIdentity
import io.semanticdf.audit.{QueryRequest => AuditQueryRequest}

/** Tests for engine-identity in the cache key (added in v0.3.0).
  *
  * Per design \u00a74.5.5 + design §11 closure: a Spark
  * request and a Trino request for the same model must produce
  * DIFFERENT cache keys \u2014 otherwise a Spark result could be
  * returned to a Trino caller (or vice versa). */
class EngineIdentityCacheKeySpec extends AnyFunSuite with Matchers {

  private val sparkEngine = EngineIdentity(
    name                 = "spark",
    nativeVersion        = "3.5.8",
    engineAdapterVersion = "0.2.4",
  )
  private val trinoEngine = EngineIdentity(
    name                 = "trino",
    nativeVersion        = "0.286",
    engineAdapterVersion = "0.2.4",
  )

  private def baseReq(engine: Option[EngineIdentity]): AuditQueryRequest =
    AuditQueryRequest(
      engine     = engine,
      model      = "orders",
      version    = 1,
      measures   = Seq("amount"),
      dimensions = Seq("region"),
    )

  // -- The cross-engine isolation property --

  test("a Spark request and a Trino request for the same model produce DIFFERENT cache keys") {
    val sparkKey = CacheKey.forRequest(baseReq(Some(sparkEngine)))
    val trinoKey = CacheKey.forRequest(baseReq(Some(trinoEngine)))
    sparkKey shouldBe defined
    trinoKey shouldBe defined
    sparkKey should not be trinoKey
  }

  test("two Spark requests with the same engine produce the SAME cache key") {
    val key1 = CacheKey.forRequest(baseReq(Some(sparkEngine)))
    val key2 = CacheKey.forRequest(baseReq(Some(sparkEngine)))
    key1 shouldBe key2
  }

  test("a request with engine = None produces a DIFFERENT cache key than engine = Some(...)") {
    val noEngineKey    = CacheKey.forRequest(baseReq(None))
    val sparkEngineKey = CacheKey.forRequest(baseReq(Some(sparkEngine)))
    noEngineKey shouldBe defined
    sparkEngineKey shouldBe defined
    noEngineKey should not be sparkEngineKey
  }

  // -- Backward-compat: old (pre-PR-4) request with no engine field still works --

  test("old (pre-PR-4) request without engine field still produces a valid cache key") {
    // Per karpathy \u00a73 ("don't refactor things that aren't broken"):
    // old code paths that build QueryRequest without the engine
    // field continue to work \u2014 they just get a different
    // cache key than engine-aware requests.
    val oldKey = CacheKey.forRequest(baseReq(None))
    oldKey shouldBe defined
    oldKey.get.nonEmpty shouldBe true
  }

  // -- Engine identity in dedup hash flows through the cache --
  // (Dedup-hash cross-engine isolation is covered by
  // AuditEventEngineFieldSpec in core. The cache key
  // forRequest test above pins the equivalent property at the
  // cache level.)
}

object EngineIdentityCacheKeySpec {
  // Marker: this test class lives in the spark module because
  // CacheKey + QueryRequest are spark-package types. The
  // engine-portable AUDIT BEHAVIOR is in core; the engine-
  // portable CACHE BEHAVIOR is in spark (which is the only
  // adapter that uses the cache today).
}