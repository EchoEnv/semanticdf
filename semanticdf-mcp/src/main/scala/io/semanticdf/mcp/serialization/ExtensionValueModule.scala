package io.semanticdf.mcp.serialization

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{BeanProperty, DeserializationContext, JsonDeserializer, JsonNode, JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import io.semanticdf.core.model.ExtensionValue

import scala.collection.JavaConverters._

/** Jackson module for round-tripping [ExtensionValue] over the wire.
  *
  * PR 11 of the 12-PR triage plan: fixes the wire-format bug where
  * `ExtensionValue.Null` did not survive JSON serialization. Before
  * this fix:
  *
  *   - `ExtensionValue.Null` serialized to JSON `{}` (because
  *     `case object` produces a no-arg constructor that Jackson
  *     treats as an empty object)
  *   - `JsonNode.VALUE_NULL` deserialized to a missing field
  *     (instead of `ExtensionValue.Null`)
  *
  * After this fix:
  *
  *   - `ExtensionValue.Null` -> JSON `null`
  *   - JSON `null` -> `ExtensionValue.Null`
  *
  * ==Why a custom serializer (not just relying on the default)==
  *
  * Jackson's default behavior for `case object Null`:
  *
  *   - Serializes to `{}` (empty object) \u2014 WRONG, should be `null`
  *
  * Jackson's default for `null` JSON:
  *
  *   - Deserializes to Java `null` \u2014 WRONG, should be `Null`
  *
  * The fix is exhaustive on add: every case of the ADT maps to a
  * distinct JSON shape. Adding a new case (e.g. `ExtensionValue.Int`)
  * would be a compile error here (the match becomes non-exhaustive).
  *
  * ==Why `BigDecimal` is `java.math.BigDecimal` (not `Double`)==
  *
  * Per ExtensionValue.scala's design comment:
  *
  *   > JSON's number type doesn't distinguish int vs float vs decimal.
  *   > `BigDecimal` covers all three losslessly; the engine adapter
  *   > narrows to its native type at the use site.
  *
  * Jackson's `node.decimalValue()` returns `java.math.BigDecimal`
  * losslessly. Using `node.asDouble()` would lose precision for
  * values like `123.456`.
  *
  * ==Boundary contract==
  *
  * Lives in `semanticdf-mcp` (the only module that depends on
  * Jackson). `semanticdf-core` deliberately does NOT depend on
  * Jackson (the data shapes are pure sealed ADTs); the wire-format
  * serialization is the transport-layer concern.
  */
object ExtensionValueModule {

  /** The serializer. Exhaustive on add \u2014 the compiler enforces
    * that every case of `ExtensionValue` is handled. */
  class Serializer extends StdSerializer[ExtensionValue](classOf[ExtensionValue]) {
    override def serialize(
        value:    ExtensionValue,
        gen:      JsonGenerator,
        provider: SerializerProvider,
    ): Unit = value match {
      case ExtensionValue.Null =>
        // CRITICAL: write JSON `null`, not `{}`. Jackson's default
        // for a case object would write `{}` (because case object
        // has no fields). Without this, the round-trip fails.
        gen.writeNull()

      case ExtensionValue.String(s) =>
        gen.writeString(s)

      case ExtensionValue.Bool(b) =>
        gen.writeBoolean(b)

      case ExtensionValue.Number(n) =>
        // DecimalNode (not DoubleNode): preserves BigDecimal
        // precision losslessly. Explicit java.math.BigDecimal
        // conversion: Jackson's writeNumber takes java.math.BigDecimal,
        // not scala.math.BigDecimal (which would route through
        // doubleValue() and lose precision).
        gen.writeNumber(n.bigDecimal)

      case ExtensionValue.List(items) =>
        gen.writeStartArray()
        items.foreach { item => serialize(item, gen, provider) }
        gen.writeEndArray()

      case ExtensionValue.Object(fields) =>
        gen.writeStartObject()
        fields.foreach { case (k, v) =>
          gen.writeFieldName(k)
          serialize(v, gen, provider)
        }
        gen.writeEndObject()
    }
  }

  /** The deserializer.
    *
    * Implements [ContextualDeserializer] so that Jackson's Map /
    * Object value-deserializer delegation goes through this
    * deserializer's `getNullValue` (which returns
    * `ExtensionValue.Null` instead of Java `null`). */
  class Deserializer extends StdDeserializer[ExtensionValue](classOf[ExtensionValue])
    with com.fasterxml.jackson.databind.deser.ContextualDeserializer {

    /** CRITICAL: override Jackson's default `getNullValue` so that
      * a JSON `null` token deserializes to `ExtensionValue.Null`
      * (NOT to Java `null`). Without this, the field is silently
      * dropped on read. */
    override def getNullValue(ctxt: DeserializationContext): ExtensionValue =
      ExtensionValue.Null

    override def createContextual(
        ctxt:     DeserializationContext,
        property: BeanProperty,
    ): JsonDeserializer[ExtensionValue] = this

    override def deserialize(
        p:    JsonParser,
        ctxt: DeserializationContext,
    ): ExtensionValue = {
      // Read the current token as a tree node. This is the standard
      // Jackson pattern for type-switching deserializers.
      val node: JsonNode = p.getCodec.readTree(p)
      fromNode(node)
    }

    private def fromNode(node: JsonNode): ExtensionValue = {
      if (node == null || node.isNull) {
        // CRITICAL: JSON `null` -> ExtensionValue.Null. Without this,
        // the field would deserialize to Java null (None in a Map),
        // losing the distinction between absent and explicit-null.
        ExtensionValue.Null
      } else if (node.isTextual) {
        ExtensionValue.String(node.asText())
      } else if (node.isBoolean) {
        ExtensionValue.Bool(node.asBoolean())
      } else if (node.isNumber) {
        // CRITICAL: parse from the JSON TEXT, not from
        // `node.decimalValue()`. Jackson's `decimalValue()` routes
        // through double precision for some node types
        // (DoubleNode, IntNode, LongNode), losing BigDecimal
        // precision. Parsing the text preserves all digits.
        // NOTE: requires the mapper to have
        // `DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS`
        // enabled, which is set by `ExtensionValueModule.mapperWithModule`.
        ExtensionValue.Number(BigDecimalBridge.fromJava(
          new java.math.BigDecimal(node.asText())
        ))
      } else if (node.isArray) {
        ExtensionValue.List(node.elements().asScala.toList.map(fromNode))
      } else if (node.isObject) {
        val fields = scala.collection.mutable.Map.empty[String, ExtensionValue]
        node.fields().asScala.foreach { entry =>
          fields += (entry.getKey -> fromNode(entry.getValue))
        }
        ExtensionValue.Object(fields.toMap)
      } else {
        throw new com.fasterxml.jackson.core.JsonParseException(
          null, s"Unexpected JSON node for ExtensionValue: $node",
        )
      }
    }
  }

  /** The Jackson module. Consumers register this to enable
    * round-trip support for [ExtensionValue].
    *
    * Usage:
    * {{{
    *   val mapper = new ObjectMapper()
    *   mapper.registerModule(DefaultScalaModule)
    *   mapper.registerModule(ExtensionValueModule.module)
    * }}}
    */
  val module: SimpleModule = {
    val m = new SimpleModule("ExtensionValueModule")
    m.addSerializer(classOf[ExtensionValue], new Serializer)
    m.addDeserializer(classOf[ExtensionValue], new Deserializer)
    m
  }

  /** Build a fresh `ObjectMapper` with the `ExtensionValueModule`
    * registered AND `USE_BIG_DECIMAL_FOR_FLOATS` enabled.
    *
    * CRITICAL: the BigDecimal feature is required for lossless
    * round-trip of `ExtensionValue.Number` values with more than
    * 16 significant digits. Without it, Jackson parses JSON
    * floating-point literals as `double` (losing precision),
    * then `node.asText()` returns the double-rounded string.
    *
    * Consumers who need to round-trip `ExtensionValue.Number`
    * should use this mapper (or enable the feature on their own
    * mapper). */
  def mapperWithModule(): com.fasterxml.jackson.databind.ObjectMapper = {
    val om = new com.fasterxml.jackson.databind.ObjectMapper()
    om.registerModule(DefaultScalaModule)
    om.registerModule(module)
    om.configure(
      com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS,
      true,
    )
    om
  }
}

/** Build a `scala.math.BigDecimal` from a `java.math.BigDecimal`.
  *
  * Helper to bridge Jackson's `node.decimalValue()` (returns
  * `java.math.BigDecimal`) into the `scala.math.BigDecimal` type
  * used by [ExtensionValue.Number.v]. */
private object BigDecimalBridge {
  def fromJava(jbd: java.math.BigDecimal): scala.math.BigDecimal =
    scala.math.BigDecimal(jbd)
}