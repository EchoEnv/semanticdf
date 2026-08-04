package io.semanticdf.core.model

/** Engine-portable extension-value ADT — Phase 2 contract. Mirrors
  * the design doc §4.4.1 "ExtensionValue" (closed enumeration of
  * JSON-shaped values for portable model extensions).
  *
  * ==Why a sealed ADT (vs. `Any` or a `Map[String, Any]`)==
  *
  * `ExtensionValue` is the portable shape of model-level metadata
  * (annotations, tags, custom fields). It's intentionally CLOSED
  * per the design's finding 12: "no `Any`, class tag, callback, or
  * engine object". The closed ADT forces:
  *   - The model validator (Group 3c) to handle every case explicitly
  *   - The MCP wire format to serialize every case (no `Any` ->
  *     engine-specific encoder)
  *   - The engine adapter's compile step to handle every case
  *     (no engine-specific callback)
  *
  * Per scala-data-driven-refactor §3 ("A rule becomes data only when
  * it must change without a deploy"): the case set is FIXED at
  * compile time (Null / String / Bool / Number / List / Object), so
  * a sealed ADT is correct, NOT a Map.
  *
  * ==Why `Null` is a separate case object==
  *
  * Per the design's round-3 DE finding 8.1: "A JSON member written
  * as `"field": null` round-trips to `Null`, never to absence
  * (absence is a different wire state). Without this case, fields
  * explicitly set to null would lose information on read because
  * they share no value with Nil."
  *
  * Canonical encoding: Jackson `JsonNode.VALUE_NULL`. A `null`
  * extension value is DIFFERENT from an absent extension field.
  *
  * ==Why `Number` is `BigDecimal` (not `Double` or `Long`)==
  *
  * JSON's number type doesn't distinguish int vs float vs decimal.
  * `BigDecimal` covers all three losslessly; the engine adapter
  * narrows to its native type (Spark `LongType` / `DoubleType` /
  * `DecimalType`, Trino `BIGINT` / `DOUBLE` / `DECIMAL`) at the
  * use site.
  *
  * Per the design's risk #9 (decimal scale/overflow): engines must
  * report `EngineError.DecimalOverflow` when a `BigDecimal` doesn't
  * fit the engine's declared precision. The portable value is
  * lossless; the engine reports overflow at the boundary.
  *
  * ==Why `List` and `Object` are recursive (not `Seq[Any]`)==
  *
  * Recursive cases (a `List` of `ExtensionValue`s, an `Object` of
  * named `ExtensionValue`s) keep the ADT closed. An `Any` would
  * re-introduce the open type the design explicitly rejected.
  *
  * ==Why core (engine-portable)==
  *
  * `ExtensionValue` is the portable shape that flows through the
  * v2 manifest and the MCP wire format. Every JSON-shaped value
  * the user can declare (a description string, a tag list, a
  * custom object) is one of these 6 cases.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 6 case classes / case objects
  * - Equality auto-derived (case classes — recursive, structural)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/ExtensionValue.scala`
  */
sealed trait ExtensionValue extends Product with Serializable

object ExtensionValue {

  /** The JSON value `null`. Distinct from an ABSENT field. Per
    * round-3 DE finding 8.1. Maps to Jackson's `JsonNode.VALUE_NULL`,
    * Trino's `null` parameter. */
  case object Null extends ExtensionValue

  /** A JSON string. Maps to Spark's `StringType`, Trino's `VARCHAR`
    * parameter. */
  final case class String(v: java.lang.String) extends ExtensionValue

  /** A JSON boolean. Maps to Spark's `BooleanType`, Trino's
    * `BOOLEAN` parameter. */
  final case class Bool(v: Boolean) extends ExtensionValue

  /** A JSON number (int, float, or decimal). Lossless via
    * `BigDecimal`. Maps to Spark's `LongType` / `DoubleType` /
    * `DecimalType` (engine narrows), Trino's `BIGINT` / `DOUBLE`
    * / `DECIMAL`. Per the design's risk #9, engines must report
    * `EngineError.DecimalOverflow` on loss-of-precision. */
  final case class Number(v: BigDecimal) extends ExtensionValue

  /** A JSON array (homogeneous or heterogeneous — the design
    * doesn't constrain element type). Maps to Spark's `ArrayType`,
    * Trino's `ARRAY`. */
  final case class List(items: scala.collection.immutable.List[ExtensionValue]) extends ExtensionValue

  /** A JSON object (a map of field names to extension values).
    * Maps to Spark's `StructType`, Trino's `ROW`. */
  final case class Object(
      fields: scala.collection.immutable.Map[java.lang.String, ExtensionValue],
  ) extends ExtensionValue
}