package io.semanticdf.core.manifest

import scala.annotation.tailrec

/** Typed parse errors for the portable manifest reader.
  *
  * Per docs/design/error-handling-style.md "Hard bans":
  *   - NO `Either[String, _]` in any code path.
  *   - All sealed error ADTs use SPECIFIC cases (no generic
  *     `ParseError(String)`).
  *
  * Each case carries the data needed to format a stable,
  * programmatic human-readable message via `.message`.
  *
  * ==Why a sealed ADT (vs. exception or string)==
  *
  * The reader (3A.2) converts YAML to `core.Model`. Failures can
  * happen at 3 distinct stages:
  *   1. **YAML syntax** (Jackson parse error)
  *   2. **Shape mismatch** (required field missing, wrong type)
  *   3. **Domain validation** (after conversion, `Model.of`
  *      rejects the model)
  *
  * Each stage has a distinct ADT case. The caller can pattern-match
  * on the case to decide what to do (e.g., retry on syntax error,
  * surface to user on shape mismatch, log+continue on validation
  * error).
  *
  * ==Why `path` is a `List[String]` (not a String)==
  *
  * Jackson's nested-path errors look like
  * `("flights", "dimensions", 2, "expr")` — a tuple of field names,
  * an array index, and a field name. The reader builds this path
  * incrementally as it walks the YAML tree. `List[String]` preserves
  * the full nesting (vs. dot-joined String which loses array indices).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/manifest/ManifestError.scala` */
sealed trait ManifestError extends Product with Serializable {
  /** Stable human-readable message for stderr / logs. */
  def message: String
}

object ManifestError {

  /** YAML parse failure (Jackson couldn't read the file). */
  final case class YamlSyntaxError(reason: String, path: List[String] = Nil) extends ManifestError {
    val message: String = path match {
      case Nil    => s"YAML syntax error: $reason"
      case nonNil => s"YAML syntax error at ${nonNil.mkString(".")}: $reason"
    }
  }

  /** A required field is missing. */
  final case class MissingField(field: String, path: List[String] = Nil) extends ManifestError {
    val message: String = path match {
      case Nil    => s"missing required field: $field"
      case nonNil => s"missing required field '$field' at ${path.mkString(".")}"
    }
  }

  /** A field has the wrong type (e.g., String where Int was expected). */
  final case class TypeMismatch(field: String, expected: String, actual: String, path: List[String] = Nil) extends ManifestError {
    val message: String = path match {
      case Nil    => s"type mismatch on $field: expected $expected, got $actual"
      case nonNil => s"type mismatch on $field at ${path.mkString(".")}: expected $expected, got $actual"
    }
  }

  /** A field's value isn't one of the expected enum values. */
  final case class InvalidEnumValue(field: String, value: String, allowed: Set[String], path: List[String] = Nil) extends ManifestError {
    val message: String = path match {
      case Nil    => s"invalid value '$value' for $field: must be one of [${allowed.mkString(", ")}]"
      case nonNil => s"invalid value '$value' for $field at ${path.mkString(".")}: must be one of [${allowed.mkString(", ")}]"
    }
  }

  /** A field's value violates a constraint (e.g., empty string, negative number). */
  final case class InvalidValue(field: String, reason: String, path: List[String] = Nil) extends ManifestError {
    val message: String = path match {
      case Nil    => s"invalid value for $field: $reason"
      case nonNil => s"invalid value for $field at ${path.mkString(".")}: $reason"
    }
  }

  /** Domain validation failed after conversion (e.g., `Model.of` rejected the result). */
  final case class DomainValidation(reason: String, modelName: String) extends ManifestError {
    val message: String = s"model '$modelName' failed domain validation: $reason"
  }

  /** Multiple errors collected during a single read pass.
    * The reader (3A.2) aggregates errors instead of failing fast,
    * so users see ALL problems in one shot. */
  final case class Multiple(errors: List[ManifestError]) extends ManifestError {
    val message: String = errors match {
      case Nil      => "no errors"
      case nonEmpty =>
        val header = s"${nonEmpty.size} error(s):"
        val body   = nonEmpty.map(e => s"  - ${e.message}").mkString("\n")
        s"$header\n$body"
    }
  }

  /** Filter conversion deferred to a future PR.
    *
    * Per the v0.3.2 design doc: portable YAML holds raw SQL strings
    * for filter expressions. The current `core.expr.Expr` ADT has
    * no `SqlString` variant — all variants are structured (FieldRef,
    * Literal, Equal, etc.). A future PR will add raw-SQL support
    * (either as a new Expr variant or a parser). Until then, manifests
    * with `filters:` entries fail loud with this typed error.
    *
    * Per the standard's "fail loud" pattern: never silently drop
    * user data. Surface the limitation so users know to remove or
    * migrate the filters. */
  final case class FilterConversionUnsupported(filterCount: Int) extends ManifestError {
    val message: String =
      s"filter conversion is not supported yet ($filterCount filter(s) in YAML); " +
      s"either remove them or wait for v0.3.3 (see v0.3.2 design doc)"
  }
}
