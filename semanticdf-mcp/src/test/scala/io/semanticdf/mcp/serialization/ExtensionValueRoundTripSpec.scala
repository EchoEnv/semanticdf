package io.semanticdf.mcp.serialization

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.semanticdf.core.model.ExtensionValue

/** Tests for the [ExtensionValueModule] round-trip.
  *
  * PR 11 of the 12-PR triage plan: pins the wire-format contract
  * that `ExtensionValue.Null` round-trips correctly. Before this
  * fix, `Null` serialized to JSON `{}` (empty object) and JSON
  * `null` deserialized to Java null (lost the explicit-null vs
  * absent distinction). */
class ExtensionValueRoundTripSpec extends AnyFunSuite with Matchers {

  // -- Test fixture: a mapper with the module registered AND
  //    USE_BIG_DECIMAL_FOR_FLOATS enabled (required for lossless
  //    BigDecimal round-trip) --

  private val mapper: ObjectMapper = ExtensionValueModule.mapperWithModule()

  private def roundTrip(value: ExtensionValue): ExtensionValue = {
    val json = mapper.writeValueAsString(value)
    mapper.readValue(json, classOf[ExtensionValue])
  }

  // -- 6 cases round-trip individually --

  test("Null round-trips losslessly (the CRITICAL fix)") {
    val out = roundTrip(ExtensionValue.Null)
    out shouldBe ExtensionValue.Null
    // Pin the JSON shape: must be `null`, not `{}`.
    val json = mapper.writeValueAsString(ExtensionValue.Null)
    json shouldBe "null"
  }

  test("String round-trips") {
    val v = ExtensionValue.String("hello world")
    roundTrip(v) shouldBe v
  }

  test("Bool round-trips") {
    roundTrip(ExtensionValue.Bool(true)) shouldBe ExtensionValue.Bool(true)
    roundTrip(ExtensionValue.Bool(false)) shouldBe ExtensionValue.Bool(false)
  }

  test("Number round-trips (lossless BigDecimal)") {
    val v = ExtensionValue.Number(BigDecimal("123.456789012345678901234567890"))
    val out = roundTrip(v)
    out shouldBe v
  }

  test("Number round-trips integer values") {
    val v = ExtensionValue.Number(BigDecimal(42))
    roundTrip(v) shouldBe v
  }

  test("List round-trips") {
    val v = ExtensionValue.List(List(
      ExtensionValue.String("a"),
      ExtensionValue.Number(BigDecimal(1)),
      ExtensionValue.Bool(true),
      ExtensionValue.Null,
    ))
    roundTrip(v) shouldBe v
  }

  test("Object round-trips") {
    val v = ExtensionValue.Object(Map(
      "name"    -> ExtensionValue.String("alice"),
      "age"     -> ExtensionValue.Number(BigDecimal(30)),
      "active"  -> ExtensionValue.Bool(true),
      "deleted" -> ExtensionValue.Null,
    ))
    roundTrip(v) shouldBe v
  }

  test("nested List<Object> round-trips") {
    val v = ExtensionValue.List(List(
      ExtensionValue.Object(Map(
        "id"   -> ExtensionValue.Number(BigDecimal(1)),
        "tags" -> ExtensionValue.List(List(ExtensionValue.String("a"), ExtensionValue.String("b"))),
      )),
      ExtensionValue.Object(Map(
        "id"   -> ExtensionValue.Number(BigDecimal(2)),
        "tags" -> ExtensionValue.Null,  // explicit null inside nested struct
      )),
    ))
    roundTrip(v) shouldBe v
  }

  // -- JSON shape verification --

  test("Null serializes to JSON `null` (not `{}`)") {
    val json = mapper.writeValueAsString(ExtensionValue.Null)
    json shouldBe "null"
  }

  test("Bool serializes to JSON true/false") {
    mapper.writeValueAsString(ExtensionValue.Bool(true)) shouldBe "true"
    mapper.writeValueAsString(ExtensionValue.Bool(false)) shouldBe "false"
  }

  test("List serializes to JSON array") {
    val json = mapper.writeValueAsString(ExtensionValue.List(List(
      ExtensionValue.String("a"),
      ExtensionValue.Number(BigDecimal(1)),
    )))
    json shouldBe """["a",1]"""
  }

  test("Object serializes to JSON object") {
    val json = mapper.writeValueAsString(ExtensionValue.Object(Map(
      "name" -> ExtensionValue.String("alice"),
      "age"  -> ExtensionValue.Number(BigDecimal(30)),
    )))
    // JSON object key order is not guaranteed; verify both fields
    json should include ("\"name\":\"alice\"")
    json should include ("\"age\":30")
  }

  test("nested Null inside Object round-trips through JSON") {
    // The CRITICAL scenario: explicit-null fields must round-trip,
    // not be lost as absent.
    val original = ExtensionValue.Object(Map(
      "present"  -> ExtensionValue.String("x"),
      "explicit" -> ExtensionValue.Null,  // explicit-null
    ))
    val json = mapper.writeValueAsString(original)
    json should include ("\"explicit\":null")

    val parsed = mapper.readValue(json, classOf[ExtensionValue])
    parsed shouldBe original
  }

  // -- Module can be registered on a fresh mapper --

  test("ExtensionValueModule can be registered independently") {
    val om = new ObjectMapper()
    om.registerModule(ExtensionValueModule.module)
    om.configure(
      com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS,
      true,
    )
    val json = om.writeValueAsString(ExtensionValue.Null)
    json shouldBe "null"
  }
}