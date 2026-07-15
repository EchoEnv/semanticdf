package io.semanticdf.mcp.handlers

import io.semanticdf.{Dimension, JoinInfo, Measure, SemanticFilter, SemanticTable}
import io.semanticdf.mcp.{Models, OkfCache}
import io.semanticdf.tools.{OkfGen}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Tests for the `describe_model` handler — the second MCP tool shipped
  * (MCP-1b). The handler is a thin adapter over `SemanticTable`'s public
  * accessors (joins / measureKind / sourceTable / filters / dimensions /
  * measures / version / name / description — all from PR #6 and PR #17).
  *
  * Like `ListModelsSpec`, we stub the `Models` registry directly — no YAML,
  * no SparkSession required beyond constructing the stubs. The OkfCache is
  * stubbed directly via the package-private constructor. */
class DescribeModelSpec extends AnyFunSuite with BeforeAndAfterAll {

  private val stubSpark: SparkSession = {
    val s = SparkSession.builder()
      .master("local[2]")
      .appName("describe-model-spec")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
    s
  }

  override protected def afterAll(): Unit = {
    if (stubSpark != null) stubSpark.stop()
  }

  // ===========================================================================
  // (1) Schema walk — version, source_table, description, dimensions, measures,
  //     joins (with one-liner summary)
  // ===========================================================================

  test("handler returns schema fields + okf_markdown when requested (joins covered by dedicated helper test)") {
    val stub = stubModel(
      key = "flights", name = "flights", description = Some("Flight facts"),
      dimensions = Seq(stubDimension("carrier", description = "Airline IATA code", isEntity = true)),
      measures = Seq(stubMeasure("flight_count", kind = "base"), stubMeasure("avg_distance", kind = "calc", siblingMeasureName = "flight_count")),
      joins = Seq.empty,    // joins need op-tree wiring; covered by buildJoinSummary tests below
      filters = Nil,
    )
    val okf = stubOkf("flights" -> "# flights\n\nOKF markdown here.\n")

    val env = new DescribeModel().handle(stub, okf, "flights", includeOkf = true)

    env.status                                  shouldBe "ok"
    env.data.model                              shouldBe "flights"
    env.data.version                            shouldBe 1
    env.data.description                        shouldBe "Flight facts"
    env.data.source_table                       shouldBe Some("flights_csv")

    env.data.dimensions.map(_.name)             shouldBe Seq("carrier")
    env.data.dimensions.head.is_entity         shouldBe true
    env.data.dimensions.head.is_time_dimension shouldBe false
    env.data.dimensions.head.description       shouldBe "Airline IATA code"

    env.data.measures.map(_.name).sorted       shouldBe Seq("avg_distance", "flight_count")
    env.data.measures.find(_.name == "flight_count").map(_.kind) shouldBe Some("base")
    env.data.measures.find(_.name == "avg_distance").map(_.kind) shouldBe Some("calc")

    env.data.joins                             shouldBe empty
    env.data.filters                            shouldBe empty
    env.data.okf_markdown                      shouldBe Some("# flights\n\nOKF markdown here.\n")
  }

  // ===========================================================================
  // (2) Filter entries — pre-join row filters surfaced with name/expr/metadata
  // ===========================================================================

  test("handler surfaces declared filters with full payload") {
    val filter = SemanticFilter(
      name        = "require_origin",
      description = Some("drop null origins"),
      expr        = "origin IS NOT NULL",
      metadata    = Map("owner" -> "data-platform-team"),
    )
    val stub = stubModel(
      key = "flights", name = "flights", description = Some("desc"),
      filters = Seq(filter),
    )
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)

    env.data.filters should have size 1
    val f = env.data.filters.head
    f.name        shouldBe "require_origin"
    f.expr        shouldBe "origin IS NOT NULL"
    f.description shouldBe "drop null origins"
    f.metadata    shouldBe Map("owner" -> "data-platform-team")
  }

  // ===========================================================================
  // (3) include_okf behaviour — false omits, cache miss returns None
  // ===========================================================================

  test("include_okf=false sets okf_markdown to None (cache is not consulted)") {
    val stub = stubModel(key = "flights", name = "flights")
    val okf  = stubOkf( /* empty */ )  // cache miss — but ignored
    new DescribeModel().handle(stub, okf, "flights", includeOkf = false)
      .data.okf_markdown shouldBe None
  }

  test("include_okf=true with no cached markdown returns None (lookup is best-effort)") {
    val stub = stubModel(key = "flights", name = "flights")
    val okf  = stubOkf( /* empty */ )  // cache miss
    new DescribeModel().handle(stub, okf, "flights", includeOkf = true)
      .data.okf_markdown shouldBe None
  }

  // ===========================================================================
  // (4) Join one-liner format — single key, composite key, cross
  // ===========================================================================

  test("join summary handles single key") {
    DescribeModel.buildJoinSummary("one", "flights", "carriers", List("carrier")) shouldBe
      "flights.carrier → carriers.carrier (one)"
  }

  test("join summary handles composite key with two+ keys") {
    DescribeModel.buildJoinSummary("many", "orders", "line_items", List("order_id", "line_id")) shouldBe
      "orders.(order_id, line_id) → line_items.(order_id, line_id) (many)"
  }

  test("join summary for cross join emits empty-key form") {
    DescribeModel.buildJoinSummary("cross", "a", "b", List.empty) shouldBe
      "a. → b. (cross)"
  }

  // ===========================================================================
  // (5) Unknown model — ModelNotFoundError carries name + available list
  // ===========================================================================

  test("handler throws ModelNotFoundError when the model is not in the registry") {
    val stub = stubModel(key = "flights", name = "flights")
    val ex = intercept[DescribeModel.ModelNotFoundError] {
      new DescribeModel().handle(stub, stubOkf(), "ghost", includeOkf = true)
    }
    ex.name       shouldBe "ghost"
    ex.available  should include ("flights")
    ex.getMessage should include ("ghost")
    ex.getMessage should include ("flights")
  }

  // ===========================================================================
  // (6) Time dimension is surfaced with smallest_time_grain
  // ===========================================================================

  test("time dimension carries is_time_dimension=true and smallest_time_grain") {
    val stub = stubModel(
      key = "flights", name = "flights",
      dimensions = Seq(stubDimension("flight_date", isTime = true, smallestGrain = Some("day"))),
    )
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    val d = env.data.dimensions.head
    d.is_time_dimension   shouldBe true
    d.smallest_time_grain shouldBe Some("day")
  }

  // ===========================================================================
  // Helpers
  // ===========================================================================

  private def stubModel(
      key: String,
      name: String,
      description: Option[String] = None,
      dimensions: Seq[Dimension] = Seq.empty,
      measures: Seq[Measure] = Seq.empty,
      joins: Seq[JoinInfo] = Seq.empty,
      filters: Seq[SemanticFilter] = Seq.empty,
  ): Models = {
    // Build the SemanticTable directly via toSemanticTable (the public entry
    // point). We don't actually need the DataFrame for describe_model — only
    // the accessors — but `toSemanticTable` requires a non-empty df. The
    // minimal valid df below has the field-count matching the dimension/measure
    // names we surface, so any future accessor that reads it will not blow up.
    val s = stubSpark
    import s.implicits._
    val empty = Seq.empty[(String, String)].toDF("k", "v")
    val table = io.semanticdf.toSemanticTable(
      table = empty,
      name = Some(name),
      description = description,
      sourceTable = Some("flights_csv"),
    )
    // The model returned by `toSemanticTable` is empty (no dims/measures).
    // For the tests we care about the accessors, not op-tree wiring, so we
    // attach dimensions/measures via withDimensions/withMeasures — which the
    // describe_model handler reads through `t.dimensions` / `t.measures`.
    val enriched = dimensions.foldLeft(table)((t, d) => t.withDimensions(d))
      .withMeasures(measures.toArray: _*)
    // `joins` and `filters` are not directly attachable without op-tree
    // plumbing. For the join tests we instead exercise the join-summary helper
    // directly (see the dedicated tests above); for filter tests we attach
    // them via withRowFilter.
    val finalTable = filters.foldLeft(enriched)((t, f) =>
      t.withRowFilter(f.name, f.expr,
        description = f.description,
        metadata = f.metadata),
    )
    val registry: Map[String, SemanticTable] = Map(key -> finalTable.version(1))
    new Models(registry, io.semanticdf.mcp.DataConfig(entries = Map.empty))
  }

  /** Build an `OkfCache` directly via the package-private constructor. */
  private def stubOkf(entries: (String, String)*): OkfCache =
    new OkfCache(entries.toMap)

  private def stubDimension(
      name: String,
      description: String = "",
      isEntity: Boolean = false,
      isTime: Boolean = false,
      smallestGrain: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
  ): Dimension = new Dimension(
    name = name,
    expr = (scope: io.semanticdf.SemanticScope) => scope(name),
    description = Some(description),
    metadata = metadata,
    isEntity = isEntity,
    isTimeDimension = isTime,
    isEventTimestamp = false,
    smallestTimeGrain = smallestGrain,
  )

  private def stubMeasure(
      name: String,
      description: Option[String] = None,
      metadata: Map[String, String] = Map.empty,
      // Drives the expr shape: a "calc" measure references a sibling measure
      // by name so `SemanticTable.measureKind(name)` classifies it as Calc;
      // a "base" measure references a plain column and stays Base. The probe
      // name for calc MUST resolve to a sibling — otherwise `MeasureProbeScope`
      // ignores the reference and the measure stays Base.
      kind: String = "base",
      siblingMeasureName: String = "sibling_measure",
  ): Measure = {
    val expr: io.semanticdf.SemanticScope => org.apache.spark.sql.Column =
      if (kind == "calc") (scope) => scope(siblingMeasureName)
      else                  (scope) => scope(name)
    Measure(name = name, expr = expr, description = description, metadata = metadata)
  }

  private def stubJoin(
      cardinality: String,
      left: String,
      right: String,
      keys: Seq[String] = Seq.empty,
      extraDims: Seq[String] = Seq.empty,
      extraMeasures: Seq[String] = Seq.empty,
  ): JoinInfo = JoinInfo(
    cardinality = cardinality,
    leftName = Some(left),
    rightName = Some(right),
    keys = keys,
    extraDimensions = extraDims,
    extraMeasures = extraMeasures,
  )
}
