package io.semanticdf.trino

import io.semanticdf.core.predicate.{Predicate => CorePredicate}

/** Phase 2 — first piece of Trino behavior: lower a portable
  * `CorePredicate` to a Trino SQL string.
  *
  * The lowerer is a PURE FUNCTION from `CorePredicate` → `String`.
  * It does NOT execute the query (that's the JDBC driver's job),
  * does NOT connect to a Trino cluster (that's `TrinoEngine.execute`),
  * does NOT manage parameter binding (that's a future concern with
  * Trino's prepared statements).
  *
  * ==What this PR does==
  *
  *   1. `SqlLowerer.lower(p: CorePredicate): String` — the entry point
  *   2. Leaf cases (8): `Compare.Eq/Ne/Lt/Le/Gt/Ge`, `In`, `IsNull`
  *   3. Compound cases (3): `And`, `Or`, `Not` (recursive)
  *   4. The result is a single-line Trino-compatible SQL WHERE clause
  *
  * ==What is NOT in this PR==
  *
  *   - Parameter binding (no `?` placeholders yet — values are
  *     inlined). Future PR uses Trino's `PreparedStatement` with
  *     `?` placeholders.
  *   - Integration with `TrinoEngine.compile`. This PR establishes
  *     the `SqlLowerer` object; wiring it into `compile` is the
  *     next small step.
  *   - String/Contains/StartsWith/EndsWith/ArrayContains (the
  *     remaining Compare cases). Future PR.
  *
  * ==Why pure function==
  *
  * Per the data-driven mantra: the data (the predicate) is in core;
  * the behavior (how to translate it) is in the engine adapter.
  * `SqlLowerer.lower` is the function. It has no side effects, no
  * state, no engine coupling beyond producing a Trino SQL string.
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' adapters/semanticdf-trino/`
  *
  * The lowerer consumes ONLY the engine-portable `CorePredicate`
  * type. It does NOT import from `io.semanticdf.predicate` (the
  * Spark-bearing original) or any Spark class.
  */
object SqlLowerer {

  /** Lower a portable `CorePredicate` to a Trino SQL string.
    *
    * Examples (run mentally):
    *   `Eq("carrier", "AA")`          →  `"carrier" = 'AA'`
    *   `Gt("distance", 500)`           →  `"distance" > 500`
    *   `And(Eq("c", "AA"), Gt("d", 1))`  →  `("carrier" = 'AA' AND "distance" > 1)`
    *   `Not(Eq("c", "AA"))`            →  `NOT ("carrier" = 'AA')`
    *
    * The output is a single-line SQL string suitable for use in
    * a `WHERE` clause. Field names are double-quoted (Trino's
    * standard identifier delimiter). String values are single-quoted.
    * Numeric values are inlined as-is.
    *
    * This is the MINIMAL viable lowerer — supports all 8 leaf cases
    * plus the 3 compound cases. Extended cases (Contains, StartsWith,
    * etc.) and parameter binding land in follow-up PRs.
    */
  def lower(p: CorePredicate): String = p match {
    // -- leaf cases: Compare family --
    case CorePredicate.Compare.Eq(field, value)   => s""""$field" = ${renderValue(value)}"""
    case CorePredicate.Compare.Ne(field, value)   => s""""$field" <> ${renderValue(value)}"""
    case CorePredicate.Compare.Lt(field, value)   => s""""$field" < ${renderValue(value)}"""
    case CorePredicate.Compare.Le(field, value)   => s""""$field" <= ${renderValue(value)}"""
    case CorePredicate.Compare.Gt(field, value)   => s""""$field" > ${renderValue(value)}"""
    case CorePredicate.Compare.Ge(field, value)   => s""""$field" >= ${renderValue(value)}"""

    // -- leaf cases: In / IsNull --
    case CorePredicate.In(field, values, negate) =>
      val op = if (negate) "NOT IN" else "IN"
      s""""$field" $op (${values.map(renderValue).mkString(", ")})"""
    case CorePredicate.IsNull(field, negate) =>
      val op = if (negate) "IS NOT NULL" else "IS NULL"
      s""""$field" $op"""

    // -- compound cases (recursive) --
    case CorePredicate.And(children @ _*) =>
      if (children.isEmpty) "TRUE"  // empty AND = always true
      else children.map(lower).mkString("(", " AND ", ")")
    case CorePredicate.Or(children @ _*) =>
      if (children.isEmpty) "FALSE"  // empty OR = always false
      else children.map(lower).mkString("(", " OR ", ")")
    case CorePredicate.Not(inner) =>
      s"NOT (${lower(inner)})"
  }

  /** Render a value for inclusion in a Trino SQL literal.
    *
    * String values are single-quoted; embedded single quotes are
    * escaped by doubling (Trino's standard SQL escape). Numeric
    * values are inlined as-is. Booleans become `TRUE` / `FALSE`.
    * Null becomes the SQL `NULL` literal.
    *
    * This is intentionally simple — Trino supports more
    * sophisticated value handling (parameter binding, type
    * inference) but those are future work.
    */
  private def renderValue(v: Any): String = v match {
    case null            => "NULL"
    case s: String       => s"'${s.replace("'", "''")}'"
    case b: Boolean      => if (b) "TRUE" else "FALSE"
    case n: Number       => n.toString
    case other           => s"'${other.toString.replace("'", "''")}'"
  }
}