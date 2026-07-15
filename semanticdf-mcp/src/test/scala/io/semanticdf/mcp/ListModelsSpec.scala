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

  test("handler lists each model with name + description, sorted alphabetically") {
    val stub = stubModels(Seq(
      ("flights",  "Flight facts",                 Some("per-flight distance and passenger counts")),
      ("carriers", "Airline carrier reference",    Some("lookup table for carrier codes")),
      ("orders",   "Order facts",                  None),  // no description in YAML
    ))

    val env = new ListModels().handle(stub)
    env.status                          shouldBe "ok"
    env.data.models.map(_.name)         shouldBe Seq("carriers", "flights", "orders")
    env.data.models.map(_.description)  shouldBe Seq(
      "lookup table for carrier codes",
      "per-flight distance and passenger counts",
      "",  // missing description → empty string (not null), JSON-friendly
    )
  }

  test("envelope is the JSON shape the contract specifies") {
    val stub = stubModels(Seq(("flights", "flights", Some("desc"))))
    val env  = new ListModels().handle(stub)

    // The agent will parse this exact shape. Pin the field set so a
    // future refactor can't accidentally drop a field the agent depends on.
    env.productElementNames.toSet      shouldBe Set("status", "data", "warnings", "meta")
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Build a `Models` registry from a list of (file-key, semantic-name, description)
    * triples — does NOT load from YAML, does NOT touch Spark. */
  private def stubModels(
      models: Seq[(String, String, Option[String])]
  ): Models = {
    val registry: Map[String, io.semanticdf.SemanticTable] = models.map { case (key, semanticName, desc) =>
      key -> stubTable(semanticName, desc)
    }.toMap
    val emptyDataConfig = DataConfig(entries = Map.empty)
    new Models(registry, emptyDataConfig)
  }

  /** Build a minimal `SemanticTable` — only the `name` and `description`
    * accessors are read by `list_models`. */
  private def stubTable(name: String, description: Option[String]): io.semanticdf.SemanticTable = {
    val s = spark
    import s.implicits._
    val emptyDf = Seq.empty[(String, String)].toDF("k", "v")
    io.semanticdf.toSemanticTable(
      table       = emptyDf,
      name        = Some(name),
      description = description,
    )
  }
}

/** Local SparkSession lifecycle — mirrors `io.semanticdf.SparkSessionFixture`
  * (the library's test fixture), kept local because test-source dependencies
  * don't flow between sibling Maven projects. */
trait SparkFixture extends BeforeAndAfterAll { this: Suite =>
  @transient private var _spark: SparkSession = _

  protected def spark: SparkSession = _spark

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder()
      .master("local[2]")
      .appName("semanticdf-mcp-tests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try { if (_spark != null) _spark.stop() } finally { _spark = null; super.afterAll() }
  }
}
