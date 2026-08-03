package io.semanticdf.core.predicate

/** Engine-portable Predicate AST — Phase 1 increment 1.
  *
  * Mirrors `io.semanticdf.predicate.Predicate` (the Spark-bearing original)
  * stripped of every method that requires a Spark type. The shape — sealed
  * trait + case classes + factory — is identical; only behavior that referenced
  * `org.apache.spark.sql.Column`, `SemanticScope`, or `org.apache.spark.sql.functions.{array_contains, lit}`
  * is omitted.
  *
  * ==Why this exists==
  *
  * The downstream multi-engine design ([[`io.semanticdf.docs.design.multi-engine-design`]])
  * carves a `semanticdf-core` artifact off the existing library. That artifact's
  * contract requires `Predicate` shape to compile without Spark on the classpath.
  * The Spark compiler lives in the adapter layer (`io.semanticdf.predicate.Predicate`),
  * whose `compile(scope): Column` stays put until consolidation (Phase 2+).
  *
  * ==Data model (parse-don't-validate, per `scala-data-driven-refactor` step 1-2)==
  *
  * Every leaf carries exactly what its semantics require:
  *   - Compare family: `(field: String, value: Any)`
  *   - `In(field, values: Seq[Any], negate: Boolean)`
  *   - `IsNull(field, negate: Boolean)`
  *   - `And(children: Seq[Predicate])` / `Or(children: Seq[Predicate])` / `Not(pred)`
  *
  * No method on these case classes requires an engine. `fields: Set[String]` and
  * `describe: String` are derived from the case-class fields alone. Per the
  * `scala-data-driven-refactor` mantra, anything else (column compilation, source
  * resolution, execution) lives in an engine adapter.
  *
  * ==Circular NOTE for reviewers==
  *
  * This file compiles with zero `org.apache.spark.*` imports. The class file
  * round-trips through Scala 2.13 + the stdlib + `java.io.Serializable`. We pay a
  * temporary duplication with the Spark-bearing `Predicate`; consolidation is
  * scheduled in a later Phase 2 PR.
  */
sealed trait Predicate extends Product with Serializable {

  /** Field names referenced by this predicate (for WHERE/HAVING routing). */
  def fields: Set[String]

  /** Human-readable description of this predicate (for observability logging). */
  def describe: String

  /** Combine with another predicate via AND. */
  def and(other: Predicate): Predicate = Predicate.And(this, other)

  /** Combine with another predicate via OR. */
  def or(other: Predicate): Predicate = Predicate.Or(this, other)

  /** Negate this predicate. */
  def not: Predicate = Predicate.Not(this)
}

object Predicate {

  // -------------------------------------------------------------------------
  // Leaf predicates
  // -------------------------------------------------------------------------

  /** Two-arg comparison predicate — the base trait for `field op value`.
    *
    * Operator encoded in the concrete subtype, not in a string field, so the
    * type system catches operator typos.
    */
  sealed trait Compare extends Predicate {
    def field: String
    def value: Any

    override def fields: Set[String] = Set(field)
  }

  object Compare {

    /** Build a Compare by operator string. Dispatches to the matching sealed case.
      *
      * Same op-string vocabulary as the Spark-bearing original. Unknown operators
      * throw `IllegalArgumentException` with the same message as the prior path.
      */
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
      override def describe: String = s"$field = $value"
    }
    final case class Ne(field: String, value: Any) extends Compare {
      override def describe: String = s"$field != $value"
    }
    final case class Lt(field: String, value: Any) extends Compare {
      override def describe: String = s"$field < $value"
    }
    final case class Le(field: String, value: Any) extends Compare {
      override def describe: String = s"$field <= $value"
    }
    final case class Gt(field: String, value: Any) extends Compare {
      override def describe: String = s"$field > $value"
    }
    final case class Ge(field: String, value: Any) extends Compare {
      override def describe: String = s"$field >= $value"
    }

    /** `field contains value` */
    final case class Contains(field: String, value: Any) extends Compare {
      override def describe: String = s"$field contains $value"
    }

    /** `field startsWith value` */
    final case class StartsWith(field: String, value: Any) extends Compare {
      override def describe: String = s"$field starts with $value"
    }

    /** `field endsWith value` */
    final case class EndsWith(field: String, value: Any) extends Compare {
      override def describe: String = s"$field ends with $value"
    }

    /** `array_contains(field, value)` — uses Spark's array_contains function,
      * which is a pure data element to carry. Behaviour lives downstream. */
    final case class ArrayContains(field: String, value: Any) extends Compare {
      override def describe: String = s"array_contains($field, $value)"
    }
  }

  /** `field in values` (or `not in` when negated). */
  final case class In(field: String, values: Seq[Any], negate: Boolean = false) extends Predicate {
    override def fields: Set[String] = Set(field)
    override def describe: String = {
      val vs = values.mkString("(", ", ", ")")
      if (negate) s"$field NOT IN $vs" else s"$field IN $vs"
    }
  }

  /** Null check (or not-null when negated). */
  final case class IsNull(field: String, negate: Boolean = false) extends Predicate {
    override def fields: Set[String] = Set(field)
    override def describe: String =
      if (negate) s"$field IS NOT NULL" else s"$field IS NULL"
  }

  // -------------------------------------------------------------------------
  // Compound predicates
  // -------------------------------------------------------------------------

  /** Conjunction. Zero or more children; an `And()` with empty varargs is `EMPTY`. */
  final case class And(children: Predicate*) extends Predicate {
    override def fields: Set[String] = children.flatMap(_.fields).toSet
    override def describe: String =
      if (children.isEmpty) "TRUE"
      else children.map(_.describe).mkString("(", " AND ", ")")
  }

  /** Disjunction. Zero or more children. */
  final case class Or(children: Predicate*) extends Predicate {
    override def fields: Set[String] = children.flatMap(_.fields).toSet
    override def describe: String =
      if (children.isEmpty) "FALSE"
      else children.map(_.describe).mkString("(", " OR ", ")")
  }

  /** Negation. */
  final case class Not(predicate: Predicate) extends Predicate {
    override def fields: Set[String] = predicate.fields
    override def describe: String = s"NOT (${predicate.describe})"
  }

  // -------------------------------------------------------------------------
  // WHERE/HAVING routing (data-only operations)
  // -------------------------------------------------------------------------

  /** Does this predicate reference any known measure?
    *
    * Implementation: collect the predicate's field names; if any name (or its
    * last dot-separated segment, since measure refs in YAML are `model.field`)
    * intersects the known-measures set, this predicate is a measure predicate.
    */
  def referencesMeasure(pred: Predicate, knownMeasures: Set[String]): Boolean = {
    val bare     = pred.fields
    val stripped = bare.map(f => if (f.contains('.')) f.split('.').last else f)
    (bare ++ stripped).intersect(knownMeasures).nonEmpty
  }

  /** Split a predicate into (pre-aggregation, post-aggregation) child predicates.
    *
    * - `And` compounds: each child is split independently and routed by bucket.
    *   Multiple pre-agg children become one `And`; same for post-agg.
    * - `Or` / `Not` / leaf: if it references any measure → whole predicate goes
    *   post-agg; otherwise → pre-agg.
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
      val preGrouped  = if (pre.size <= 1) pre  else Seq(And(pre: _*))
      val postGrouped = if (post.size <= 1) post else Seq(And(post: _*))
      (preGrouped, postGrouped)

    case _ =>
      if (referencesMeasure(pred, knownMeasures)) (Nil, Seq(pred))
      else                                        (Seq(pred), Nil)
  }

  /** Negation helper. */
  def not(pred: Predicate): Predicate = Not(pred)
}
