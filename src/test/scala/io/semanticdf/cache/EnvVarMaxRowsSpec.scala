package io.semanticdf.cache

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the `SEMANTICDF_MAX_ROWS` env-var override on
  * `CacheBridge.executeQuery` (post-v0.2.2 follow-up implementing
  * the existing Scaladoc TODO at `CacheBridge.scala:88`).
  *
  * The env-var lets operators raise the row cap without
  * recompiling. The default is `CacheKey.DefaultMaxRows`
  * (100,000). The override is a single `Int` that overrides the
  * default for the 5-arg `executeQuery` overload.
  */
class EnvVarMaxRowsSpec extends AnyFunSuite with Matchers {
  test("parseEnvMaxRows: returns None when env-var is unset") {
    // The pure parser doesn't read the ambient env — it operates
    // on the `env: Map[String, String]` parameter. Unset means
    // the map doesn't contain the key.
    assert(CacheBridge.parseEnvMaxRows(Map.empty) == None)
  }

  test("parseEnvMaxRows: parses valid positive int") {
    assert(CacheBridge.parseEnvMaxRows(Map("SEMANTICDF_MAX_ROWS" -> "500000")) == Some(500000))
  }

  test("parseEnvMaxRows: parses 0 (no-cap sentinel)") {
    assert(CacheBridge.parseEnvMaxRows(Map("SEMANTICDF_MAX_ROWS" -> "0")) == Some(0))
  }

  test("parseEnvMaxRows: rejects negative int (must be non-negative)") {
    assert(CacheBridge.parseEnvMaxRows(Map("SEMANTICDF_MAX_ROWS" -> "-1")) == None)
  }

  test("parseEnvMaxRows: rejects non-numeric value") {
    assert(CacheBridge.parseEnvMaxRows(Map("SEMANTICDF_MAX_ROWS" -> "abc")) == None)
  }

  test("parseEnvMaxRows: rejects empty string") {
    assert(CacheBridge.parseEnvMaxRows(Map("SEMANTICDF_MAX_ROWS" -> "")) == None)
  }

  test("parseEnvMaxRows: Int.MaxValue accepted (the field is Int)") {
    assert(CacheBridge.parseEnvMaxRows(Map("SEMANTICDF_MAX_ROWS" -> "2147483647")) == Some(Int.MaxValue))
  }

  test("parseEnvMaxRows: overflow rejected (must be a valid Int)") {
    assert(CacheBridge.parseEnvMaxRows(Map("SEMANTICDF_MAX_ROWS" -> "9999999999999")) == None)
  }

  // ----------------------------------------------------------------
  // effectiveMaxRows integration: precedence rule (override > env > default)
  // ----------------------------------------------------------------

  test("effectiveMaxRows precedence: test-override > env-var > library default") {
    // Save and restore the test-only override. Production never
    // sets this; we mutate it via the spec's try/finally to keep
    // tests isolated.
    val originalOverride = CacheBridge.envMaxRowsOverride
    try {
      // Default fallback: no env, no override.
      CacheBridge.envMaxRowsOverride = None
      assert(CacheBridge.effectiveMaxRows == CacheBridge.DefaultMaxRows)
      // Test-override wins over the env-var path: the override
      // is the highest-precedence source.
      CacheBridge.envMaxRowsOverride = Some(7)
      assert(CacheBridge.effectiveMaxRows == 7)
    } finally {
      CacheBridge.envMaxRowsOverride = originalOverride
    }
  }
}
