package io.semanticdf.core.model

/** Engine-portable extension-limits ADT — Phase 2 contract.
  * Mirrors the design doc §4.4.1 "ExtensionLimits" (constants
  * enforced by the validator in Group 3c).
  *
  * ==Why constants on a singleton object==
  *
  * The limits are PORTABLE (every engine applies them the same
  * way). They're not behavior; they're INVARIANTS. Per scala-data-
  * driven-refactor §1 ("data is data, behavior lives elsewhere"):
  * the invariants are declared here; the ENFORCEMENT is in the
  * validator (`ModelValidator`, Group 3c).
  *
  * ==Why 8 KiB + 16 fields (not larger)==
  *
  * Per the design: "Limits apply to canonical UTF-8 JSON; the 16
  * fields are counted recursively. Larger payloads are fully
  * externalized as content-addressed blobs."
  *
  * The 8 KiB + 16 fields limit keeps the model portable — the
  * extensions fit in a reasonable JSON payload (a typical manifest
  * row), so the v2 manifest can carry them without external
  * dependencies. Larger payloads MUST be externalized via
  * [[ExternalExtensionBlob]].
  *
  * ==Why `check(extensions)` returns `Either[Excess, Unit]`==
  *
  * The validator (Group 3c) calls `check(extensions)` to verify
  * the inline envelope fits within limits. If it exceeds, the
  * `Left` carries `Excess(fieldCount, byteCount)` for the
  * validator to map to
  * `ModelValidationError.ExtensionEnvelopeExceeded(fieldCount,
  * byteCount)`.
  *
  * ==Why a case class for `Excess` (not a tuple)==
  *
  * `Excess` is a structured error value. It carries named fields
  * (the field count and byte count). A `Tuple2[Int, Int]` would
  * be untyped — readers would have to remember which is which.
  * The case class makes the error value self-documenting.
  *
  * ==Why core (engine-portable)==
  *
  * The limits are universal — every engine enforces the same
  * invariant. The ENFORCEMENT (calling `check`) is engine-agnostic
  * (it's pure-data analysis). No engine-specific logic here.
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: `object` (singleton) + `final case class Excess`
  * - Equality auto-derived (case class)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/model/ExtensionLimits.scala`
  */
object ExtensionLimits {

  /** Maximum number of bytes for the inline envelope (after
    * canonical UTF-8 encoding). Per the design. */
  val MaxInlineBytes: Int = 8 * 1024

  /** Maximum number of fields (counted recursively) for the inline
    * envelope. Per the design. */
  val MaxFields: Int = 16

  /** The result of an `ExtensionLimits.check` call.
    *
    * @param fieldCount the actual field count (recursive)
    * @param byteCount  the actual byte count (canonical UTF-8)
    */
  final case class Excess(fieldCount: Int, byteCount: Int)
      extends Product with Serializable

  /** Verify that an inline envelope fits within [[MaxFields]] +
    * [[MaxInlineBytes]]. Returns `Right(())` if it fits, or
    * `Left(Excess(...))` with the actual counts if it doesn't.
    *
    * The validator (Group 3c) calls this. Per the design's finding
    * 14, larger payloads are externalized (the model carries an
    * [[ExternalExtensionBlob]] instead of an inline envelope) —
    * never truncated.
    *
    * @param extensions the inline extension map to check
    * @return `Right(())` if fits, `Left(Excess(...))` if exceeds */
  def check(extensions: Map[String, ExtensionValue]): Either[Excess, Unit] = {
    val fieldCount = countFields(extensions)
    val byteCount  = byteSize(extensions)
    if (fieldCount > MaxFields || byteCount > MaxInlineBytes)
      Left(Excess(fieldCount = fieldCount, byteCount = byteCount))
    else
      Right(())
  }

  /** Recursively count the number of fields. The map's top-level
    * size, plus the recursive size of every `List` and `Object`
    * value. */
  private def countFields(extensions: Map[String, ExtensionValue]): Int = {
    def countValue(v: ExtensionValue): Int = v match {
      case ExtensionValue.Null                          => 0
      case _: ExtensionValue.String                    => 0
      case _: ExtensionValue.Bool                      => 0
      case _: ExtensionValue.Number                    => 0
      case ExtensionValue.List(items)                  => items.iterator.map(countValue).sum
      case ExtensionValue.Object(fields)               => fields.size + fields.values.iterator.map(countValue).sum
    }
    extensions.size + extensions.values.iterator.map(countValue).sum
  }

  /** Compute the byte size (canonical UTF-8 encoding) of the
    * inline envelope. The map's top-level size, plus the recursive
    * size of every `List` and `Object` value (each element's
    * canonical UTF-8 byte length). Field names are also counted. */
  private def byteSize(extensions: Map[String, ExtensionValue]): Int = {
    def sizeOf(v: ExtensionValue): Int = v match {
      case ExtensionValue.Null                          => 4  // "null"
      case s: ExtensionValue.String                    => s.v.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 2  // quotes
      case b: ExtensionValue.Bool                      => if (b.v) 4 else 5  // "true" / "false"
      case n: ExtensionValue.Number                    => n.v.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
      case ExtensionValue.List(items)                  => items.iterator.map(sizeOf).sum + 2  // brackets
      case ExtensionValue.Object(fields)               =>
        fields.iterator.map { case (k, v) =>
          k.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 2 + sizeOf(v) + 1  // key + colon + comma
        }.sum + 1  // closing brace
    }
    extensions.iterator.map { case (k, v) =>
      k.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 2 + sizeOf(v) + 1  // key + colon + comma
    }.sum + 1  // closing brace
  }
}