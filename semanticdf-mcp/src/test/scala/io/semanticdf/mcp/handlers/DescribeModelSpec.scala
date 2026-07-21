package io.semanticdf.mcp.handlers

import io.semanticdf.{Dimension, JoinInfo, Measure, SemanticFilter, SemanticTable}
import io.semanticdf.mcp.{Models, OkfCache, SparkFixture}
import io.semanticdf.tools.{OkfGen}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for the `describe_model` handler — the second MCP tool shipped
  * (MCP-1b). The handler is a thin adapter over `SemanticTable`'s public
  * accessors (joins / measureKind / sourceTable / filters / dimensions /
  * measures / version / name / description — all from PR #6 and PR #17).
  *
  * Like `ListModelsSpec`, we stub the `Models` registry directly — no YAML
  * needed. The OkfCache is stubbed directly via the package-private
  * constructor. Uses the shared `SparkFixture` so all MCP specs share one
  * SparkSession per JVM run. */
class DescribeModelSpec extends AnyFunSuite with SparkFixture {

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
  // (3b) Lifecycle status — describe_model surfaces ModelStatus as wire string
  // ===========================================================================

  test("handler surfaces status: 'published' by default") {
    val stub = stubModel(key = "flights", name = "flights")
    new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
      .data.status shouldBe "published"
  }

  test("handler surfaces status: 'draft' when set on the model") {
    val stub = stubModel(key = "flights", name = "flights",
      status = io.semanticdf.ModelStatus.Draft)
    new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
      .data.status shouldBe "draft"
  }

  test("handler surfaces status: 'deprecated' when set on the model") {
    val stub = stubModel(key = "flights", name = "flights",
      status = io.semanticdf.ModelStatus.Deprecated)
    new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
      .data.status shouldBe "deprecated"
  }

  // ===========================================================================
  // (3c) Lifecycle warnings — describe_model carries warnings on the envelope
  // ===========================================================================

  test("describe-warns-deprecated: envelope warnings include deprecation string") {
    val stub = stubModel(key = "flights", name = "flights",
      status = io.semanticdf.ModelStatus.Deprecated)
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    env.warnings shouldBe List("model 'flights' is deprecated")
  }

  test("describe-warns-draft: envelope warnings include the draft-specific wording") {
    val stub = stubModel(key = "flights", name = "flights",
      status = io.semanticdf.ModelStatus.Draft)
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    env.warnings shouldBe List("model 'flights' is in draft; shape may change")
  }

  test("describe-does-not-warn-published: published model returns warnings == Nil") {
    val stub = stubModel(key = "flights", name = "flights",
      status = io.semanticdf.ModelStatus.Published)
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    env.warnings shouldBe Nil
  }

  test("draft-warning-text-distinct-from-deprecated: both wordings differ") {
    val draftStub = stubModel(key = "draft_m", name = "draft_m",
      status = io.semanticdf.ModelStatus.Draft)
    val depStub   = stubModel(key = "old_m", name = "old_m",
      status = io.semanticdf.ModelStatus.Deprecated)
    val draftWarn = new DescribeModel().handle(draftStub, stubOkf(), "draft_m", includeOkf = false).warnings
    val depWarn   = new DescribeModel().handle(depStub,   stubOkf(), "old_m",   includeOkf = false).warnings
    draftWarn shouldBe List("model 'draft_m' is in draft; shape may change")
    depWarn   shouldBe List("model 'old_m' is deprecated")
    (draftWarn ++ depWarn).distinct.size shouldBe 2  // two distinct strings
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
      status: io.semanticdf.ModelStatus = io.semanticdf.ModelStatus.Published,
  ): Models = {
    // Build the SemanticTable directly via toSemanticTable (the public entry
    // point). We don't actually need the DataFrame for describe_model — only
    // the accessors — but `toSemanticTable` requires a non-empty df. The
    // minimal valid df below has the field-count matching the dimension/measure
    // names we surface, so any future accessor that reads it will not blow up.
    val s = spark
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
    val registry: Map[String, SemanticTable] = Map(key -> finalTable.version(1).status(status))
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
      exprString: Option[String] = None,
  ): Dimension = new Dimension(
    name = name,
    expr = (scope: io.semanticdf.SemanticScope) => scope(name),
    description = Some(description),
    metadata = metadata,
    isEntity = isEntity,
    isTimeDimension = isTime,
    isEventTimestamp = false,
    smallestTimeGrain = smallestGrain,
    exprString = exprString,
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
      exprString: Option[String] = None,
  ): Measure = {
    val expr: io.semanticdf.SemanticScope => org.apache.spark.sql.Column =
      if (kind == "calc") (scope) => scope(siblingMeasureName)
      else                  (scope) => scope(name)
    Measure(name = name, expr = expr, description = description, metadata = metadata, exprString = exprString)
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

  // ===========================================================================
  // (7) expr field surfaces the original expression string when available
  // ===========================================================================
  //
  // Regression: before PR (feat/describe-model-expr-string), Dimension/Measure
  // stored only a `SemanticScope => Column` lambda, so DescribeModel serialised
  // `expr` via `expr.toString` and produced opaque lambda addresses
  // (`io.semanticdf.YamlLoader$$$Lambda$...`). The fix: Dimension and Measure
  // now carry an optional `exprString` (populated by the YamlLoader from the
  // YAML `expr:` value; can also be set programmatically). DescribeModel
  // prefers the string when present and falls back to `toString` otherwise
  // (back-compat for consumers that build dims/measures with bare lambdas).

  test("describe_model surfaces Dimension.exprString when set (the new field)") {
    val stub = stubModel(
      key = "flights", name = "flights",
      dimensions = Seq(
        stubDimension("carrier", exprString = Some("carrier")),
      ),
      measures = Seq.empty,
    )
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    env.data.dimensions.head.expr shouldBe "carrier"
  }

  test("describe_model surfaces Measure.exprString when set (the new field)") {
    val stub = stubModel(
      key = "flights", name = "flights",
      dimensions = Seq.empty,
      measures = Seq(stubMeasure("flight_count", exprString = Some("count(*)"))),
    )
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    env.data.measures.head.expr shouldBe "count(*)"
  }

  test("describe_model falls back to lambda toString when exprString is absent (back-compat)") {
    // Programmatic consumers that build dims/measures with bare lambdas get
    // the old opaque output. This is intentional — we don't have a source
    // string to surface. New consumers can opt in by passing exprString.
    val stub = stubModel(
      key = "flights", name = "flights",
      dimensions = Seq(stubDimension("carrier")), // no exprString
      measures = Seq(stubMeasure("flight_count")), // no exprString
    )
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    val dExpr = env.data.dimensions.head.expr
    val mExpr = env.data.measures.head.expr
    // The lambda toString contains the anonymous-class marker or @hash;
    // either way it's clearly NOT the original 'carrier' / 'flight_count'.
    assert(dExpr != "carrier",  s"expected lambda fallback, got: $dExpr")
    assert(mExpr != "flight_count", s"expected lambda fallback, got: $mExpr")
  }

  test("describe_model: dim exprString with simple identifier (column reference)") {
    val stub = stubModel(
      key = "flights", name = "flights",
      dimensions = Seq(stubDimension("origin", exprString = Some("origin"))),
      measures = Seq.empty,
    )
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    env.data.dimensions.head.expr shouldBe "origin"
  }

  test("describe_model: measure exprString with a complex expression") {
    // YAML example: `expr: \"sum(distance) / count(*)\"`.
    val complexExpr = "sum(distance) / count(*)"
    val stub = stubModel(
      key = "flights", name = "flights",
      dimensions = Seq.empty,
      measures = Seq(stubMeasure("avg_distance", kind = "calc", exprString = Some(complexExpr))),
    )
    val env = new DescribeModel().handle(stub, stubOkf(), "flights", includeOkf = false)
    env.data.measures.head.expr shouldBe complexExpr
  }
}
