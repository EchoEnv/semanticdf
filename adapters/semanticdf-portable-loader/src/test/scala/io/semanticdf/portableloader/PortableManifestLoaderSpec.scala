package io.semanticdf.portableloader

import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.manifest.ManifestError
import io.semanticdf.core.rel.{AggregateFn, JoinKind}
import io.semanticdf.core.model.{ModelStatus, SourceRef}

/** v0.3.2 (Step 3 PR #A.2): tests for the portable manifest loader.
  *
  * Asserts the public API:
  *   `PortableManifestLoader.load(path): Either[ManifestError, core.Model]`
  *
  * Per karpathy §4 (verifiable goals): tests assert the actual
  * `core.Model` shape (not just "loaded"), per
  * `scala-spark-batch-bugs §1`.
  *
  * Per the standard: assert typed `ManifestError` cases (not just
  * "returned Left"). */
class PortableManifestLoaderSpec extends AnyFunSuite with Matchers {

  private def writeTempYaml(content: String): Path = {
    val tmp = Files.createTempFile("manifest-", ".yml")
    Files.write(tmp, content.getBytes("UTF-8"))
    tmp
  }

  // -- Happy path: minimal valid YAML --

  test("load: minimal valid YAML → Right(Model)") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  catalog: hive
        |  namespace: sales
        |  table: orders
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val result = PortableManifestLoader.load(path)
      result.isRight shouldBe true
      val model = result.toOption.get
      model.name shouldBe "flights"
      model.source shouldBe SourceRef.ByName(
        catalog = Some("hive"), namespace = Some("sales"), table = "orders"
      )
    } finally Files.deleteIfExists(path)
  }

  // -- Missing required field: name --

  test("load: missing name → ManifestError.MissingField") {
    val yaml =
      """source:
        |  type: ByName
        |  table: orders
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val result = PortableManifestLoader.load(path)
      result shouldBe Left(ManifestError.MissingField(field = "name"))
    } finally Files.deleteIfExists(path)
  }

  // -- Missing required field: source --

  test("load: missing source → ManifestError.MissingField") {
    val yaml = """name: flights"""
    val path = writeTempYaml(yaml)
    try {
      val result = PortableManifestLoader.load(path)
      result shouldBe Left(ManifestError.MissingField(field = "source"))
    } finally Files.deleteIfExists(path)
  }

  // -- Filter conversion deferred (limitation #4) --

  test("load: filters present → ManifestError.FilterConversionUnsupported") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |filters:
        |  - name: require_recent
        |    expr: "created_at > '2024-01-01'"
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val result = PortableManifestLoader.load(path)
      val err = result.swap.toOption.get
      err shouldBe a [ManifestError.FilterConversionUnsupported]
      err.message should include("filter conversion is not supported")
    } finally Files.deleteIfExists(path)
  }

  // -- Status mapping --

  test("load: status: published → ModelStatus.Published") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |status: published
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val result = PortableManifestLoader.load(path)
      result.toOption.get.status shouldBe ModelStatus.Published
    } finally Files.deleteIfExists(path)
  }

  // -- Dimension conversion (limitation #1: FieldRef simplification) --

  test("load: dimensions → List[Dimension] with FieldRef") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |dimensions:
        |  - name: carrier
        |    expr: carrier
        |  - name: origin
        |    expr: origin
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val model = PortableManifestLoader.load(path).toOption.get
      model.dimensions.size shouldBe 2
      model.dimensions.map(_.name).toSet shouldBe Set("carrier", "origin")
    } finally Files.deleteIfExists(path)
  }

  // -- Measure conversion: kind mapping --

  test("load: measures with kind → List[Measure] with AggregateFn") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |measures:
        |  - name: flight_count
        |    expr: "1"
        |    kind: count
        |  - name: total_passengers
        |    expr: passengers
        |    kind: sum
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val model = PortableManifestLoader.load(path).toOption.get
      model.measures.size shouldBe 2
      val countMeasure = model.measures.find(_.name == "flight_count").get
      countMeasure.expr match {
        case ac: io.semanticdf.core.rel.AggregateCall =>
          ac.fn shouldBe AggregateFn.Count
        case other =>
          fail(s"expected AggregateCall, got: $other")
      }
    } finally Files.deleteIfExists(path)
  }

  test("load: measures with unknown kind → AggregateFn.Sum (limitation #2)") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |measures:
        |  - name: x
        |    expr: x_col
        |    kind: unknown_aggregate_xyz
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val model = PortableManifestLoader.load(path).toOption.get
      model.measures.head.expr match {
        case ac: io.semanticdf.core.rel.AggregateCall =>
          ac.fn shouldBe AggregateFn.Sum
        case other =>
          fail(s"expected fallback to AggregateFn.Sum, got: $other")
      }
    } finally Files.deleteIfExists(path)
  }

  // -- Join conversion: kind mapping --

  test("load: joins with kind → List[JoinSpec] with JoinKind") {
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
    val path = writeTempYaml(yaml)
    try {
      val model = PortableManifestLoader.load(path).toOption.get
      model.joins.size shouldBe 1
      model.joins.head.kind shouldBe JoinKind.Inner  // "many" → Inner per limitation #3
    } finally Files.deleteIfExists(path)
  }

  // -- Rollup conversion: grain normalization (limitation #5) --

  test("load: rollups with grain → List[RollupSpec] with TimeGrain.normalize") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |rollups:
        |  - name: daily_flights
        |    grain: DAY
        |    measures:
        |      - flight_count
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val model = PortableManifestLoader.load(path).toOption.get
      model.rollups.size shouldBe 1
      model.rollups.head.name shouldBe "daily_flights"
    } finally Files.deleteIfExists(path)
  }

  test("load: rollups with invalid grain → ManifestError.InvalidEnumValue") {
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |rollups:
        |  - name: bad_grain_rollup
        |    grain: FORTNIGHT
        |    measures: [flight_count]
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val err = PortableManifestLoader.load(path).swap.toOption.get
      err shouldBe a [ManifestError.InvalidEnumValue]
      err.message should include("FORTNIGHT")
    } finally Files.deleteIfExists(path)
  }

  // -- Domain validation failure --

  test("load: dimension/measure name collision → ManifestError.DomainValidation") {
    // Per ModelValidator case (2a): a measure name that collides with
    // a dimension name is rejected. Duplicate measures alone are not
    // checked by the validator (separate concern).
    val yaml =
      """name: flights
        |source:
        |  type: ByName
        |  table: orders
        |dimensions:
        |  - name: dup
        |    expr: dup_col
        |measures:
        |  - name: dup
        |    expr: a
        |""".stripMargin
    val path = writeTempYaml(yaml)
    try {
      val err = PortableManifestLoader.load(path).swap.toOption.get
      err shouldBe a [ManifestError.DomainValidation]
      err.message should include("duplicate")
    } finally Files.deleteIfExists(path)
  }

  // -- Syntax error --

  test("load: malformed YAML → ManifestError.YamlSyntaxError") {
    val yaml = "name: flights\nsource:\n  type: ByName\n table: [unclosed\n"
    val path = writeTempYaml(yaml)
    try {
      val err = PortableManifestLoader.load(path).swap.toOption.get
      err shouldBe a [ManifestError.YamlSyntaxError]
    } finally Files.deleteIfExists(path)
  }

  // -- File not found --

  test("load: non-existent file → Left(MissingField)") {
    val path = Path.of("/tmp/does-not-exist-yml-12345.yml")
    val err = PortableManifestLoader.load(path).swap.toOption.get
    err shouldBe a [ManifestError.MissingField]
  }

  // -- loadString: in-memory YAML --

  test("loadString: in-memory YAML → Right(Model)") {
    val yaml =
      """name: in_memory
        |source:
        |  type: ByName
        |  table: orders
        |""".stripMargin
    val result = PortableManifestLoader.loadString(yaml)
    result.isRight shouldBe true
    result.toOption.get.name shouldBe "in_memory"
  }
}
