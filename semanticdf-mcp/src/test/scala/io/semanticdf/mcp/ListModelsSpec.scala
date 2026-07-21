package io.semanticdf.mcp

import io.semanticdf.mcp.handlers.ListModels
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Unit test for the `list_models` handler.
  *
  * The handler is a pure function of the loaded `Models` registry — no
  * Spark, no JSON, no SDK. We test the data shape directly, which is the
  * contract the agent actually consumes.
  *
  * Integration with the MCP SDK (transport wiring, JSON-RPC framing) is
  * out of scope here; that's covered by the SDK's own test suite and
  * verified end-to-end by running the server against a real client
  * (manual smoke test, see `semanticdf-mcp/README.md`). */
class ListModelsSpec extends AnyFunSuite with BeforeAndAfterAll with SparkFixture {
  ListModelsSpec => // make the trait's protected spark visible in this class

  test("handler returns empty list when no models are loaded") {
    val empty = stubModels(Seq.empty)
    val env   = new ListModels().handle(empty)
    env.status                        shouldBe "ok"
    env.warnings                      shouldBe List.empty[String]
    env.data.models                   shouldBe List.empty[ListModels#ModelSummary]
  }

  test("handler lists each model with name + description + status, sorted alphabetically") {
    val stub = stubModels(Seq(
      ("flights",  "Flight facts",                 Some("per-flight distance and passenger counts"), Some(io.semanticdf.ModelStatus.Published)),
      ("carriers", "Airline carrier reference",    Some("lookup table for carrier codes"),         Some(io.semanticdf.ModelStatus.Deprecated)),
      ("orders",   "Order facts",                  None,                                           Some(io.semanticdf.ModelStatus.Draft)),
    ))

    val env = new ListModels().handle(stub)
    env.status                          shouldBe "ok"
    env.data.models.map(_.name)         shouldBe Seq("carriers", "flights", "orders")
    env.data.models.map(_.description)  shouldBe Seq(
      "lookup table for carrier codes",
      "per-flight distance and passenger counts",
      "",  // missing description → empty string (not null), JSON-friendly
    )
    env.data.models.map(_.status)       shouldBe Seq("deprecated", "published", "draft")
  }

  test("envelope is the JSON shape the contract specifies") {
    val stub = stubModels(Seq(("flights", "flights", Some("desc"), None)))
    val env  = new ListModels().handle(stub)

    // The agent will parse this exact shape. Pin the field set so a
    // future refactor can't accidentally drop a field the agent depends on.
    env.productElementNames.toSet      shouldBe Set("status", "data", "warnings", "meta")
  }

  // ============================================================================
  // Lifecycle warnings + status surfacing (PR: feat/lifecycle-enforcement)
  // ============================================================================

  test("list-models-carries-status: every ModelSummary has a status string") {
    val stub = stubModels(Seq(
      ("flights",  "flights",  None, Some(io.semanticdf.ModelStatus.Published)),
      ("orders",   "orders",   None, Some(io.semanticdf.ModelStatus.Deprecated)),
      ("draft_m",  "draft_m",  None, Some(io.semanticdf.ModelStatus.Draft)),
    ))
    val env = new ListModels().handle(stub)
    // Sorted alphabetically by registry key: draft_m < flights < orders
    env.data.models.map(_.status) shouldBe Seq("draft", "published", "deprecated")
  }

  test("list-models-warns-deprecated: warnings include one entry per Deprecated model") {
    val stub = stubModels(Seq(
      ("flights",   "flights",   None, Some(io.semanticdf.ModelStatus.Deprecated)),
      ("orders",    "orders",    None, Some(io.semanticdf.ModelStatus.Published)),
    ))
    val env = new ListModels().handle(stub)
    env.warnings shouldBe Seq("model 'flights' is deprecated")
  }

  test("list-models-warns-draft: warnings include the draft-specific wording") {
    val stub = stubModels(Seq(
      ("flights", "flights", None, Some(io.semanticdf.ModelStatus.Draft)),
    ))
    val env = new ListModels().handle(stub)
    env.warnings shouldBe Seq("model 'flights' is in draft; shape may change")
  }

  test("list-models-warning-order-deterministic: warnings sorted alphabetically with models") {
    val stub = stubModels(Seq(
      ("flights", "flights", None, Some(io.semanticdf.ModelStatus.Deprecated)),
      ("alpha",   "alpha",   None, Some(io.semanticdf.ModelStatus.Deprecated)),
      ("beta",    "beta",    None, Some(io.semanticdf.ModelStatus.Draft)),
    ))
    val env = new ListModels().handle(stub)
    env.warnings shouldBe Seq(
      "model 'alpha' is deprecated",
      "model 'beta' is in draft; shape may change",
      "model 'flights' is deprecated",
    )
  }

  test("list-models-no-duplicate-warnings: each model warned at most once") {
    // Even if the helper ran multiple times (regression check), one warning
    // per model.
    val stub = stubModels(Seq(
      ("flights", "flights", None, Some(io.semanticdf.ModelStatus.Deprecated)),
    ))
    val env = new ListModels().handle(stub)
    env.warnings.size shouldBe 1
  }

  test("list-models-does-not-warn-published: all-Published registry -> warnings == Nil") {
    val stub = stubModels(Seq(
      ("flights",  "flights",  None, Some(io.semanticdf.ModelStatus.Published)),
      ("carriers", "carriers", None, Some(io.semanticdf.ModelStatus.Published)),
    ))
    val env = new ListModels().handle(stub)
    env.warnings shouldBe Nil
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Build a `Models` registry from a list of (file-key, semantic-name,
    * description, status) quads — does NOT load from YAML, does NOT touch
    * Spark. Pass `status = None` to use the library default (Published).
    * Use the 3-tuple overload via implicit conversion (see below) for the
    * existing tests that don't care about status. */
  private def stubModels(
      models: Seq[(String, String, Option[String], Option[io.semanticdf.ModelStatus])]
  ): Models = {
    val registry: Map[String, io.semanticdf.SemanticTable] = models.map { case (key, semanticName, desc, status) =>
      key -> stubTable(semanticName, desc, status)
    }.toMap
    val emptyDataConfig = DataConfig(entries = Map.empty)
    new Models(registry, emptyDataConfig)
  }

  /** Build a minimal `SemanticTable` — only the `name`, `description`, and
    * `status` accessors are read by `list_models`. */
  private def stubTable(name: String, description: Option[String], status: Option[io.semanticdf.ModelStatus] = None): io.semanticdf.SemanticTable = {
    val s = spark
    import s.implicits._
    val emptyDf = Seq.empty[(String, String)].toDF("k", "v")
    val base = io.semanticdf.toSemanticTable(
      table       = emptyDf,
      name        = Some(name),
      description = description,
    )
    status.fold(base)(s => base.status(s))
  }

  // Implicit conversion so 3-tuple (no status) calls still compile to the
  // 4-tuple overload. The default status is None (= library default Published).
  private implicit def to4TupleSeq(
      xs: Seq[(String, String, Option[String])]
  ): Seq[(String, String, Option[String], Option[io.semanticdf.ModelStatus])] =
    xs.map { case (k, n, d) => (k, n, d, None) }
}
