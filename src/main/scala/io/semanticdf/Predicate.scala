package io.semanticdf

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

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

  private val opSymbol: String => String = {
    case "eq" => "="
    case "ne" => "!="
    case "lt" => "<"
    case "le" => "<="
    case "gt" => ">"
    case "ge" => ">="
    case o    => o
  }

  /** Two-arg comparison: `field op value`. */
  final case class Compare(op: String, field: String, value: Any) extends Predicate {
    override def fields: Set[String] = Set(field)

    override def compile(scope: SemanticScope): Column = {
      val col = scope(field)
      val v   = lit(value)
      op match {
        case "eq"       => col === v
        case "ne"       => col =!= v
        case "lt"       => col < v
        case "le"       => col <= v
        case "gt"       => col > v
        case "ge"       => col >= v
        case other      => throw new IllegalArgumentException(s"Unknown compare op: $other")
      }
    }

    override def describe: String = s"$field ${opSymbol(op)} $value"
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

  implicit def strToField(field: String): PredicateField = new PredicateField(field)

  /** Builder returned by the `strToField` implicit conversion. */
  final class PredicateField(private val field: String) extends AnyVal {

    /** `field === value` → equality. */
    def ===(v: Any): Predicate = Compare("eq", field, v)

    /** `field =!= value` → inequality. */
    def =!=(v: Any): Predicate = Compare("ne", field, v)

    /** `field > value`. */
    def >(v: Any): Predicate = Compare("gt", field, v)

    /** `field >= value`. */
    def >=(v: Any): Predicate = Compare("ge", field, v)

    /** `field < value`. */
    def <(v: Any): Predicate = Compare("lt", field, v)

    /** `field <= value`. */
    def <=(v: Any): Predicate = Compare("le", field, v)

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
