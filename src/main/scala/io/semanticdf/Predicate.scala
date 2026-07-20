package io.semanticdf

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{array_contains, lit}

/** Predicate AST for filter expressions (Phase 5, DESIGN §6.5).
  *
  * A small algebra over filters: `Compare | In | IsNull | And | Or | Not`.
  * Every node compiles to a Spark [[Column]] via a [[SemanticScope]], and reports the
  * set of field names it references. The same node compiles correctly for either a
  * pre-aggregation table (WHERE — base columns) or a post-aggregation table (HAVING —
  * measure-name columns), because resolution is by name via the scope.
  *
  * Mirrors BSL's `predicate.py` AST. `Custom` (callable escape hatch) and JSON parsing
  * are deferred — the Scala DSL is the v0.1 surface.
  *
  * ==Usage==
  * {{{
  * import Predicate._
  *
  * st.where("carrier" === "AA")
  * st.where("total_passengers" > 600)
  * st.where(("carrier" === "AA") and ("total_passengers" > 100))
  * st.where("carrier" in ("AA", "UA"))
  * st.where("origin".isNull)
  * }}}
  */
sealed trait Predicate extends Product with Serializable {

  /** Field names referenced by this predicate (for WHERE/HAVING routing). */
  def fields: Set[String]

  /** Compile to a Spark [[Column]] against the given scope. */
  def compile(scope: SemanticScope): Column

  /** Human-readable description of this predicate (for observability logging). */
  def describe: String

  /** Combine with another predicate via AND. */
  def and(other: Predicate): Predicate = Predicate.And(this, other)

  /** Combine with another predicate via OR. */
  def or(other: Predicate): Predicate = Predicate.Or(this, other)

  /** Negate this predicate. (There is no `!pred` shorthand — `!` only works on
    * `Boolean`, and a `Predicate` is not a `Boolean`. Use `pred.not` or `not(pred)`.) */
  def not: Predicate = Predicate.Not(this)
}

object Predicate {

  // -------------------------------------------------------------------------
  // Leaf predicates
  // -------------------------------------------------------------------------

  /** Two-arg comparison predicate — the base trait for `field op value`.
    *
    * The operator (`eq`/`ne`/`lt`/`le`/`gt`/`ge`) is encoded in the concrete
    * subtype ([[Compare.Eq]], [[Compare.Ne]], [[Compare.Lt]], [[Compare.Le]],
    * [[Compare.Gt]], [[Compare.Ge]]), not in a string field. This pushes operator
    * choice to the type system — `Compare.Gt` is the only `>` constructor;
    * `Compare.Greater(...)` doesn't compile.
    *
    * `Compare.apply(op, field, value)` (the [[Compare]] companion's `apply`)
    * preserves the previous stringly-typed call site for backward compatibility.
    * It dispatches on the operator string and throws `IllegalArgumentException`
    * for unknown values, matching the prior behavior exactly.
    *
    * Compile and describe move into each case — faster (no string match on
    * every compile) and identical output (verified by tests). */
  sealed trait Compare extends Predicate {
    def field: String
    def value: Any

    override def fields: Set[String] = Set(field)
  }

  object Compare {

    /** Build a Compare by operator string. Prefer the typed sealed cases.
      *
      * Backward-compatible factory for the prior `Predicate.Compare(op, field, value)`
      * call site. Dispatches based on `op` and constructs the matching sealed
      * case class. Unknown operators throw with the same message as the prior
      * stringly-typed path. */
    def apply(op: String, field: String, value: Any): Compare = op match {
      case "eq" => Eq(field, value)
      case "ne" => Ne(field, value)
      case "lt" => Lt(field, value)
      case "le" => Le(field, value)
      case "gt" => Gt(field, value)
      case "ge" => Ge(field, value)
      case other => throw new IllegalArgumentException(s"Unknown compare op: $other")
    }

    final case class Eq(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field) === lit(value)
      override def describe: String = s"$field = $value"
    }
    final case class Ne(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field) =!= lit(value)
      override def describe: String = s"$field != $value"
    }
    final case class Lt(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field) < lit(value)
      override def describe: String = s"$field < $value"
    }
    final case class Le(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field) <= lit(value)
      override def describe: String = s"$field <= $value"
    }
    final case class Gt(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field) > lit(value)
      override def describe: String = s"$field > $value"
    }
    final case class Ge(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field) >= lit(value)
      override def describe: String = s"$field >= $value"
    }

    /** String substring search: `field contains value`. */
    final case class Contains(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field).contains(lit(value))
      override def describe: String = s"$field contains $value"
    }

    /** String prefix: `field startsWith value`. */
    final case class StartsWith(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field).startsWith(lit(value))
      override def describe: String = s"$field starts with $value"
    }

    /** String suffix: `field endsWith value`. */
    final case class EndsWith(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = scope(field).endsWith(lit(value))
      override def describe: String = s"$field ends with $value"
    }

    /** Array membership: `array_contains(field, value)`. */
    final case class ArrayContains(field: String, value: Any) extends Compare {
      override def compile(scope: SemanticScope): Column = array_contains(scope(field), lit(value))
      override def describe: String = s"array_contains($field, $value)"
    }
  }

  /** Membership test: `field in values` (or `not in` when negated). */
  final case class In(field: String, values: Seq[Any], negate: Boolean = false) extends Predicate {
    override def fields: Set[String] = Set(field)

    override def compile(scope: SemanticScope): Column = {
      val col = scope(field)
      if (negate) !col.isin(values: _*)
      else         col.isin(values: _*)
    }

    override def describe: String = {
      val vs = values.mkString("(", ", ", ")")
      if (negate) s"$field NOT IN $vs" else s"$field IN $vs"
    }
  }

  /** Null check (or not-null when negated). */
  final case class IsNull(field: String, negate: Boolean = false) extends Predicate {
    override def fields: Set[String] = Set(field)

    override def compile(scope: SemanticScope): Column = {
      val col = scope(field)
      if (negate) col.isNotNull
      else         col.isNull
    }

    override def describe: String = if (negate) s"$field IS NOT NULL" else s"$field IS NULL"
  }

  // -------------------------------------------------------------------------
  // Compound predicates
  // -------------------------------------------------------------------------

  /** Conjunction of one or more predicates. */
  final case class And(children: Predicate*) extends Predicate {
    override def fields: Set[String] = children.flatMap(_.fields).toSet

    override def compile(scope: SemanticScope): Column =
      children.map(_.compile(scope)).reduce(_ && _)

    override def describe: String = children.map(_.describe).mkString("(", " AND ", ")")
  }

  /** Disjunction of one or more predicates. */
  final case class Or(children: Predicate*) extends Predicate {
    override def fields: Set[String] = children.flatMap(_.fields).toSet

    override def compile(scope: SemanticScope): Column =
      children.map(_.compile(scope)).reduce(_ || _)

    override def describe: String = children.map(_.describe).mkString("(", " OR ", ")")
  }

  /** Negation. */
  final case class Not(predicate: Predicate) extends Predicate {
    override def fields: Set[String] = predicate.fields

    override def compile(scope: SemanticScope): Column =
      !predicate.compile(scope)

    override def describe: String = s"NOT (${predicate.describe})"
  }

  // -------------------------------------------------------------------------
  // WHERE/HAVING routing
  // -------------------------------------------------------------------------

  /** Does this predicate reference any known measure? */
  def referencesMeasure(pred: Predicate, knownMeasures: Set[String]): Boolean = {
    val bare   = pred.fields
    val stripped = bare.map(f => if (f.contains('.')) f.split('.').last else f)
    (bare ++ stripped).intersect(knownMeasures).nonEmpty
  }

  /** Split a predicate into (pre-aggregation, post-aggregation) predicates.
    *
    * - `And` compounds: each child is split independently and routed to the right bucket.
    *   Multiple pre-agg children become one `And`; multiple post-agg children become one `And`.
    * - `Or` / `Not` / leaf: if it references any measure → whole predicate goes post-agg;
    *   otherwise → pre-agg. (An `Or` mixing dimension and measure conditions cannot be
    *   split — it must evaluate as one expression, post-aggregation.)
    *
    * Returns `(preAgg, postAgg)` where each is a possibly-empty `Seq[Predicate]`.
    */
  def splitFilter(
      pred: Predicate,
      knownMeasures: Set[String],
  ): (Seq[Predicate], Seq[Predicate]) = pred match {
    case And(children@_*) =>
      val (pres, posts) = children.map(splitFilter(_, knownMeasures)).unzip
      val pre  = pres.flatten
      val post = posts.flatten
      // Re-group into at most one And per side (cleaner op tree).
      val preGrouped  = if (pre.size <= 1) pre  else Seq(And(pre: _*))
      val postGrouped = if (post.size <= 1) post else Seq(And(post: _*))
      (preGrouped, postGrouped)

    case _ =>
      if (referencesMeasure(pred, knownMeasures)) (Nil, Seq(pred))
      else                                        (Seq(pred), Nil)
  }

  // -------------------------------------------------------------------------
  // Fluent DSL
  // -------------------------------------------------------------------------

  /** Implicit conversion enabling `"field" === value`, `"field" > value`, etc.
    *
    * Uses `===` (not `==`) and `=!=` (not `!=`) to avoid collisions with Scala's
    * universal equality on `Any`. String has no `>`, `<`, `>=`, `<=` methods, so those
    * resolve cleanly via this conversion.
    */
  /** Negation function: `not("carrier" === "AA")`. */
  def not(pred: Predicate): Predicate = Not(pred)

  // -------------------------------------------------------------------------
  // Typed factories (SemanticField typeclass)
  // -------------------------------------------------------------------------
  // Each factory takes a typed `FieldRef` instead of a string name. The
  // `SemanticField[T]` context bound is satisfied by the underlying typeclass
  // witness — which is found via the user-declared `implicit val` for the
  // field. Pure adapters over the existing `Compare`/`In`/`IsNull`
  // constructors; identical runtime cost; identical output.

  /** `field === value` with a typed ref. The ref may be a dimension or a measure;
    * downstream WHERE/HAVING routing decides which gets pushed where. */
  def Eq[F](f: FieldRef[F], v: Any)(implicit ev: SemanticField[F]): Predicate =
    Compare.Eq(ev.name, v)

  /** `field != value` with a typed ref. */
  def Ne[F](f: FieldRef[F], v: Any)(implicit ev: SemanticField[F]): Predicate =
    Compare.Ne(ev.name, v)

  /** `field > value` with a typed ref. */
  def Gt[F](f: FieldRef[F], v: Any)(implicit ev: SemanticField[F]): Predicate =
    Compare.Gt(ev.name, v)

  /** `field >= value` with a typed ref. */
  def Ge[F](f: FieldRef[F], v: Any)(implicit ev: SemanticField[F]): Predicate =
    Compare.Ge(ev.name, v)

  /** `field < value` with a typed ref. */
  def Lt[F](f: FieldRef[F], v: Any)(implicit ev: SemanticField[F]): Predicate =
    Compare.Lt(ev.name, v)

  /** `field <= value` with a typed ref. */
  def Le[F](f: FieldRef[F], v: Any)(implicit ev: SemanticField[F]): Predicate =
    Compare.Le(ev.name, v)

  /** `field in (v1, v2, ...)` with a typed ref. */
  def in[F](f: FieldRef[F], values: Any*)(implicit ev: SemanticField[F]): Predicate =
    In(ev.name, values.toSeq, negate = false)

  /** `field not in (v1, v2, ...)` with a typed ref. */
  def notIn[F](f: FieldRef[F], values: Any*)(implicit ev: SemanticField[F]): Predicate =
    In(ev.name, values.toSeq, negate = true)

  /** `field.isNull` with a typed ref. */
  def isNull[F](f: FieldRef[F])(implicit ev: SemanticField[F]): Predicate =
    IsNull(ev.name, negate = false)

  /** `field.isNotNull` with a typed ref. */
  def isNotNull[F](f: FieldRef[F])(implicit ev: SemanticField[F]): Predicate =
    IsNull(ev.name, negate = true)

  implicit def strToField(field: String): PredicateField = new PredicateField(field)

  /** Builder returned by the `strToField` implicit conversion. */
  final class PredicateField(private val field: String) extends AnyVal {

    /** `field === value` → equality. */
    def ===(v: Any): Predicate = Compare.Eq(field, v)

    /** `field =!= value` → inequality. */
    def =!=(v: Any): Predicate = Compare.Ne(field, v)

    /** `field > value`. */
    def >(v: Any): Predicate = Compare.Gt(field, v)

    /** `field >= value`. */
    def >=(v: Any): Predicate = Compare.Ge(field, v)

    /** `field < value`. */
    def <(v: Any): Predicate = Compare.Lt(field, v)

    /** `field <= value`. */
    def <=(v: Any): Predicate = Compare.Le(field, v)

    /** `field in (v1, v2, ...)`. */
    def in(values: Any*): Predicate = In(field, values, negate = false)

    /** `field notIn (v1, v2, ...)`. */
    def notIn(values: Any*): Predicate = In(field, values, negate = true)

    /** `field.isNull`. */
    def isNull: Predicate = IsNull(field, negate = false)

    /** `field.isNotNull`. */
    def isNotNull: Predicate = IsNull(field, negate = true)
  }
}
