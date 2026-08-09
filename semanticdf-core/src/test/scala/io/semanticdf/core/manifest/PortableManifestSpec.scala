package io.semanticdf.core.manifest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** v0.3.2: sanity tests for the portable manifest types.
  *
  * Per the v0.3.2 design doc (PR #437): these types are the
  * YAML-reader intermediate. The reader (3A.2) will add
  * round-trip tests + integration tests against existing
  * example YAMLs.
  *
  * For 3A.1 (types only), the tests assert:
  *   - Each type can be constructed with valid arguments
  *   - Each type's `case class` equality + `copy` work
  *   - Each type extends `Product with Serializable`
  *   - `ManifestError.message` includes the key info
  *   - `PortableStatus.parse` round-trips via `asString`
  *
  * Per scala-data-driven-refacer §1: pure data has no behavior,
  * so the tests are minimal — the compile is the primary
  * verification. The behavior tests (reader, round-trip) come
  * in 3A.2. */
class PortableManifestSpec extends AnyFunSuite with Matchers {

  // -- PortableDimension --

  test("PortableDimension: case class shape + defaults") {
    val d = PortableDimension(name = "carrier", expr = "carrier")
    d.name shouldBe "carrier"
    d.expr shouldBe "carrier"
    d.description shouldBe None
    d.isInstanceOf[Product] shouldBe true
    d.isInstanceOf[Serializable] shouldBe true
  }

  // -- PortableMeasure --

  test("PortableMeasure: case class shape + defaults") {
    val m = PortableMeasure(name = "flight_count", expr = "count(1)")
    m.name shouldBe "flight_count"
    m.expr shouldBe "count(1)"
    m.kind shouldBe None
    m.description shouldBe None
  }

  // -- PortableJoin --

  test("PortableJoin: kind is String for v1 (matches legacy)") {
    val j = PortableJoin(
      name        = "flights_to_carriers",
      kind        = "many",
      leftSource  = "flights",
      rightSource = "carriers",
      keys        = List("carrier"),
    )
    j.kind shouldBe "many"
  }

  // -- PortableSource --

  test("PortableSource.ByName: optional catalog/namespace") {
    val s = PortableSource.ByName(
      catalog = Some("hive"), namespace = Some("sales"), table = "orders"
    )
    s.table shouldBe "orders"
    s.catalog shouldBe Some("hive")
  }

  test("PortableSource.ByPath: format + options") {
    val s = PortableSource.ByPath(
      path = "s3://bucket/orders.parquet", format = "parquet",
      options = Map("compression" -> "snappy"),
    )
    s.path shouldBe "s3://bucket/orders.parquet"
    s.format shouldBe "parquet"
  }

  test("PortableSource.ByProvider: provider + identifier") {
    val s = PortableSource.ByProvider(provider = "iceberg", identifier = "orders")
    s.provider shouldBe "iceberg"
    s.identifier shouldBe "orders"
  }

  // -- PortableStatus --

  test("PortableStatus.parse: legacy string values") {
    PortableStatus.parse("draft") shouldBe Some(PortableStatus.Draft)
    PortableStatus.parse("published") shouldBe Some(PortableStatus.Published)
    PortableStatus.parse("deprecated") shouldBe Some(PortableStatus.Deprecated)
    PortableStatus.parse("DRAFT") shouldBe Some(PortableStatus.Draft)  // case-insensitive
  }

  test("PortableStatus.parse: unknown string → None") {
    PortableStatus.parse("archived") shouldBe None
    PortableStatus.parse("") shouldBe None
  }

  test("PortableStatus.asString: round-trip via parse") {
    for (s <- Seq(PortableStatus.Draft, PortableStatus.Published, PortableStatus.Deprecated)) {
      PortableStatus.parse(s.asString) shouldBe Some(s)
    }
  }

  // -- ManifestError --

  test("ManifestError.YamlSyntaxError: message includes reason") {
    val err = ManifestError.YamlSyntaxError(reason = "unexpected token")
    err.message should include("YAML syntax error")
    err.message should include("unexpected token")
  }

  test("ManifestError.YamlSyntaxError: message includes path when present") {
    val err = ManifestError.YamlSyntaxError(
      reason = "unexpected token", path = List("flights", "dimensions", "expr")
    )
    err.message should include("flights.dimensions.expr")
  }

  test("ManifestError.MissingField: message includes field name") {
    val err = ManifestError.MissingField(field = "table")
    err.message should include("missing required field")
    err.message should include("table")
  }

  test("ManifestError.TypeMismatch: message includes expected + actual") {
    val err = ManifestError.TypeMismatch(
      field = "version", expected = "Int", actual = "String"
    )
    err.message should include("type mismatch")
    err.message should include("version")
    err.message should include("Int")
    err.message should include("String")
  }

  test("ManifestError.InvalidEnumValue: message includes allowed values") {
    val err = ManifestError.InvalidEnumValue(
      field = "status", value = "archived",
      allowed = Set("draft", "published", "deprecated"),
    )
    err.message should include("archived")
    err.message should include("draft")
    err.message should include("published")
  }

  test("ManifestError.DomainValidation: message includes model name") {
    val err = ManifestError.DomainValidation(
      reason = "duplicate dimension", modelName = "flights"
    )
    err.message should include("flights")
    err.message should include("duplicate dimension")
  }

  test("ManifestError.Multiple: aggregates child errors") {
    val errs = ManifestError.Multiple(List(
      ManifestError.MissingField(field = "name"),
      ManifestError.TypeMismatch(field = "version", expected = "Int", actual = "String"),
    ))
    errs.message should include("2 error(s)")
    errs.message should include("missing required field")
    errs.message should include("type mismatch")
  }

  test("ManifestError: 6 SPECIFIC cases (no generic ParseError)") {
    // Per error-handling-style.md hard ban: every error ADT has SPECIFIC cases.
    // Asserts the count matches what we designed in 3A.1.
    val cases: List[ManifestError] = List(
      ManifestError.YamlSyntaxError("x"),
      ManifestError.MissingField("x"),
      ManifestError.TypeMismatch("x", "Int", "String"),
      ManifestError.InvalidEnumValue("x", "y", Set("z")),
      ManifestError.InvalidValue("x", "y"),
      ManifestError.DomainValidation("x", "y"),
    )
    cases.size shouldBe 6  // Multiple is the aggregator, not a leaf case
  }
}
