package io.semanticdf.core.engine

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.model.Model

/** Tests for [[MCPEngineRegistry]] (added in PR 5 of the 12-PR
  * triage plan). Per the design \u00a76.4: the registry's `select`
  * filters availability, the default must be available at
  * construction, the `availableProviders` list reflects runtime
  * availability. */
class MCPEngineRegistrySpec extends AnyFunSuite with Matchers {

  // -- Test fixture: a minimal MCPEngineProvider impl --

  private final class FakeProvider(
      override val identity: EngineIdentity,
      override val available: Boolean,
  ) extends MCPEngineProvider {
    override def query(
        model: Model, request: MCPQueryRequest, ctx: EngineContext,
    ): Either[EngineError, PortableQueryResult] = ???
    override def explain(
        model: Model, request: MCPQueryRequest, ctx: EngineContext,
    ): Either[EngineError, String] = ???
  }

  // -- Construction invariants --

  test("registry construction fails if default is not in the engines map") {
    val providers = Map(
      "spark" -> new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true),
    )
    intercept[IllegalArgumentException] {
      MCPEngineRegistry(providers, default = "trino")
    }
  }

  test("registry construction fails if default is registered but unavailable") {
    val providers = Map(
      "spark" -> new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = false),
    )
    intercept[IllegalArgumentException] {
      MCPEngineRegistry(providers, default = "spark")
    }
  }

  // -- select --

  test("select returns Right(provider) for a registered + available name") {
    val spark = new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true)
    val registry = MCPEngineRegistry(Map("spark" -> spark), default = "spark")
    registry.select("spark") shouldBe Right(spark)
  }

  test("select returns Left(EngineUnavailable) for an unknown name") {
    val spark = new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true)
    val registry = MCPEngineRegistry(Map("spark" -> spark), default = "spark")
    registry.select("trino") match {
      case Left(EngineError.EngineUnavailable(name, available, wasDefault)) =>
        name shouldBe "trino"
        wasDefault shouldBe false
        available should contain ("spark")
      case other => fail(s"expected Left(EngineUnavailable), got $other")
    }
  }

  test("availableProviders lists only registered + available providers") {
    val spark = new FakeProvider(EngineIdentity("spark", "3.5.8", "0.2.4"), available = true)
    val trino = new FakeProvider(EngineIdentity("trino", "0.286", "0.2.4"), available = true)
    val down  = new FakeProvider(EngineIdentity("databricks", "13.3", "0.2.4"), available = false)
    val registry = MCPEngineRegistry(
      Map("spark" -> spark, "trino" -> trino, "databricks" -> down),
      default = "spark",
    )
    registry.availableProviders shouldBe List("spark", "trino") // sorted, no databricks
  }
}
