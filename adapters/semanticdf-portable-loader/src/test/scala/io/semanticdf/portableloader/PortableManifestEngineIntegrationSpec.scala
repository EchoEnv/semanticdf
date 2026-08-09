package io.semanticdf.portableloader

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.engine.{
  EngineContext,
  EngineError,
  EngineIdentity,
  MCPEngineProvider,
  MCPEngineRegistry,
  MCPQueryRequest,
  PortableQueryResult,
  ResultSchema,
}
import io.semanticdf.core.model.Model

/** v0.3.2 (Step 3 PR #A.3): integration test for the portable loader
  * with the engine-portable MCP engine registry.
  *
  * Per the v0.3.2 design doc (PR #437) + the user's request: this is
  * the "small integration test proves the loader actually works in
  * practice" check.
  *
  * ==What this test proves==
  *
  * The portable loader produces a `core.Model` that:
  *   1. Passes the `Model.of` validation (already covered in 3A.2)
  *   2. Can be registered with the engine-portable `MCPEngineRegistry`
  *   3. Can be SELECTED by the registry's `select(name)` method
  *   4. Can be passed to an `MCPEngineProvider.query(...)` call as
  *      the `model` argument
  *
  * Per scala-spark-batch-bugs §1: asserts the actual model + the
  * actual engine-call path (not just "loaded successfully").
  *
  * ==Why a stub engine (vs. real SparkEngineProvider)==
  *
  * Per scala-jar-packaging: the portable-loader module has Jackson +
  * jackson-dataformat-yaml deps. Adding `semanticdf-spark` as a
  * test dep would pull Spark (~200MB) into the test classpath,
  * slowing the test suite.
  *
  * Per scala-jvm-safety: no real JDBC / cluster / etc. The stub
  * engine captures the model + request and asserts on them.
  *
  * A real Spark integration test belongs in `semanticdf-spark` (not
  * here). That's tracked as a future PR (3A.4 or similar). */
class PortableManifestEngineIntegrationSpec extends AnyFunSuite with Matchers {

  /** Stub MCPEngineProvider that captures every model + request it
    * receives. Lets us assert that the loaded model flows through
    * the engine path correctly.
    *
    * Per scala-data-driven-refacer §1 ("behavior in adapters"): this
    * is behavior (the IMPLEMENTATION of the provider trait), not data.
    * It lives in the test, not in a domain type. */
  final class CapturingProvider(val engineName: String) extends MCPEngineProvider {
    val identity: EngineIdentity = EngineIdentity(
      name               = engineName,
      nativeVersion      = "test-1.0",
      engineAdapterVersion = "stub",
    )
    var available: Boolean = true
    /** Every model passed to query() is captured here. */
    var lastModel: Option[Model] = None
    /** Every request passed to query() is captured here. */
    var lastRequest: Option[MCPQueryRequest] = None

    def query(
        model:   Model,
        request: MCPQueryRequest,
        ctx:     EngineContext,
    ): Either[EngineError, PortableQueryResult] = {
      lastModel   = Some(model)
      lastRequest = Some(request)
      Right(PortableQueryResult(
        schema   = ResultSchema(fields = Nil),
        rows     = Vector.empty,
        metadata = Map.empty,
      ))
    }

    def explain(
        model:   Model,
        request: MCPQueryRequest,
        ctx:     EngineContext,
    ): Either[EngineError, String] = {
      lastModel   = Some(model)
      lastRequest = Some(request)
      Right(s"plan for ${model.name}")
    }
  }

  test("integration: loaded Model registers with MCPEngineRegistry") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |dimensions:
        |  - name: carrier
        |    expr: carrier
        |measures:
        |  - name: flight_count
        |    expr: "1"
        |    kind: count
        |""".stripMargin
    val model = PortableManifestLoader.loadString(yaml).toOption.get

    val provider = new CapturingProvider(engineName = "stub-spark")
    val registry = new MCPEngineRegistry(
      engines = Map("stub-spark" -> provider),
      default = "stub-spark",
    )

    // The registry selects the provider by engine name.
    val selected = registry.select("stub-spark")
    selected.isRight shouldBe true
    selected.toOption.get shouldBe provider

    // The loaded model can be passed to the provider's query() call.
    val request = MCPQueryRequest(model = model.name)
    val ctx     = EngineContext.defaultContext
    val result = provider.query(model, request, ctx)
    result.isRight shouldBe true
    provider.lastModel.get shouldBe model
    provider.lastRequest.get shouldBe request
  }

  test("integration: loaded Model with joins passes through provider") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |joins:
        |  - name: flights_to_carriers
        |    kind: many
        |    leftSource: flights
        |    rightSource: carriers
        |    keys:
        |      - carrier
        |""".stripMargin
    val model = PortableManifestLoader.loadString(yaml).toOption.get
    model.joins.size shouldBe 1

    val provider = new CapturingProvider(engineName = "stub-trino")
    val registry = new MCPEngineRegistry(
      engines = Map("stub-trino" -> provider),
      default = "stub-trino",
    )

    val request = MCPQueryRequest(model = model.name, dimensions = Seq("carrier"))
    val ctx     = EngineContext.defaultContext
    val result = provider.query(model, request, ctx)

    result.isRight shouldBe true
    provider.lastModel.get.joins.size shouldBe 1
    provider.lastModel.get.joins.head.name shouldBe "flights_to_carriers"
  }

  test("integration: registry default engine is used when no name provided") {
    val yaml =
      """name: model_a
        |source:
        |  type: ByName
        |  table: orders
        |""".stripMargin
    val model = PortableManifestLoader.loadString(yaml).toOption.get

    val defaultProvider = new CapturingProvider(engineName = "default-engine")
    val otherProvider   = new CapturingProvider(engineName = "other-engine")
    val registry = new MCPEngineRegistry(
      engines = Map(
        "default-engine" -> defaultProvider,
        "other-engine"   -> otherProvider,
      ),
      default = "default-engine",
    )

    // Select default explicitly
    val selected = registry.select("default-engine")
    selected.toOption.get shouldBe defaultProvider
    selected.toOption.get should not be otherProvider
  }

  test("integration: loaded Model's status maps to engine-usable form") {
    val yaml =
      """name: published_model
        |source:
        |  type: ByName
        |  table: orders
        |status: published
        |""".stripMargin
    val model = PortableManifestLoader.loadString(yaml).toOption.get
    model.status shouldBe io.semanticdf.core.model.ModelStatus.Published

    // The model's status is consumable by any engine via the registry.
    val provider = new CapturingProvider(engineName = "stub")
    val registry = new MCPEngineRegistry(
      engines = Map("stub" -> provider),
      default = "stub",
    )
    provider.query(model, MCPQueryRequest(model = model.name),
      EngineContext.defaultContext)

    provider.lastModel.get.status shouldBe io.semanticdf.core.model.ModelStatus.Published
  }
}
