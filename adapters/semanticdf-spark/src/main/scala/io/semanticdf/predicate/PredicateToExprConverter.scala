package io.semanticdf.predicate

import io.semanticdf.core.engine.EngineError
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

import java.sql.{Date => SqlDate, Timestamp => SqlTimestamp}

/** Boundary adapter: convert legacy
  * `io.semanticdf.predicate.Predicate` → portable
  * `io.semanticdf.core.expr.Expr`.
  *
  * ==Why this exists==
  *
  * Closes Gap 4 from `docs/design/v0.3.1-feature-parity-backlog.md`.
  * The v1 `ModelBridge.toModel` (PR #409, port 8a) couldn't convert
  * legacy `where` / `having` predicates — it silently set `filters = Nil`
  * producing unfiltered portable queries. PR #414 added a fail-loud
  * guard via `ModelValidationError.FilterConversionUnsupported`.
  *
  * This converter closes the gap for the common cases (Eq / Ne / Lt /
  * Le / Gt / Ge, And / Or / Not, In / IsNull, all literals). Cases
  * that have no portable counterpart yet (Contains / StartsWith /
  * EndsWith / ArrayContains) still fail loud — deferred to v0.4.0.
  *
  * ==Shape transformation==
  *
  * The legacy ADT uses positional `(field: String, value: Any)`
  * comparisons:
  *
  *     Predicate.Compare.Gt("amount", 100)
  *
  * The portable ADT uses binary `Expr` trees:
  *
  *     Expr.GreaterThan(
  *       Expr.FieldRef("amount"),
  *       Expr.Literal(LiteralValue.IntValue(100), SealedDataType.Int),
  *     )
  *
  * Conversion is therefore a structural rewrite, not a rename.
  *
  * ==Error contract==
  *
  * Returns `Either[EngineError.UnsupportedCapability, Expr]` per
  * `docs/design/error-handling-style.md`: typed `Either` at the
  * boundary, no throw/catch round-trip. The caller (`ModelBridge`)
  * chains via `flatMap` and converts `EngineError.UnsupportedCapability`
  * to `ModelValidationError.FilterConversionUnsupported` at the
  * public-API boundary.
  *
  * ==JVM-safety check 1 (Null is a liar)==
  *
  * The legacy `value: Any` may be `null` (e.g. `Predicate.IsNull("x", negate=true)`
  * uses `null` semantically, but `Compare.Eq("x", null)` is also legal).
  * The `toLiteral` helper catches `null` at the boundary and maps it
  * to `LiteralValue.NullValue` (per scala-jvm-safety §1: every
  * unmarked boundary can hand you a null).
  */
object PredicateToExprConverter {

  /** Convert a legacy `Predicate` to a portable `Expr`. Recursive
    * for And/Or/Not.
    *
    * Returns `Left(EngineError.UnsupportedCapability(name, reason))`
    * for legacy predicates that have no portable counterpart yet
    * (Contains, StartsWith, EndsWith, ArrayContains, empty In(),
    * unsupported literal types). */
  def toExpr(p: Predicate): Either[EngineError.UnsupportedCapability, Expr] = p match {
    // -------------------------------------------------------------------------
    // Compare family (8 supported + 4 deferred)
    // -------------------------------------------------------------------------
    case Predicate.Compare.Eq(field, value) =>
      toLiteral(value).map(lit => Expr.Equal(toFieldRef(field), lit))
    case Predicate.Compare.Ne(field, value) =>
      toLiteral(value).map(lit => Expr.NotEqual(toFieldRef(field), lit))
    case Predicate.Compare.Lt(field, value) =>
      toLiteral(value).map(lit => Expr.LessThan(toFieldRef(field), lit))
    case Predicate.Compare.Le(field, value) =>
      toLiteral(value).map(lit => Expr.LessOrEqual(toFieldRef(field), lit))
    case Predicate.Compare.Gt(field, value) =>
      toLiteral(value).map(lit => Expr.GreaterThan(toFieldRef(field), lit))
    case Predicate.Compare.Ge(field, value) =>
      toLiteral(value).map(lit => Expr.GreaterOrEqual(toFieldRef(field), lit))

    case Predicate.Compare.Contains(_, _) =>
      unsupported("Predicate.Compare.Contains")
    case Predicate.Compare.StartsWith(_, _) =>
      unsupported("Predicate.Compare.StartsWith")
    case Predicate.Compare.EndsWith(_, _) =>
      unsupported("Predicate.Compare.EndsWith")
    case Predicate.Compare.ArrayContains(_, _) =>
      unsupported("Predicate.Compare.ArrayContains")

    // -------------------------------------------------------------------------
    // Non-compare leaves
    // -------------------------------------------------------------------------
    // In(field, values, negate) -> Or-chain of Equal (or Not of Or if negate).
    // For the common case of a single value, this collapses to Eq/Ne.
    case Predicate.In(field, values, negate) =>
      if (values.isEmpty) {
        // Empty In(): SQL semantics is FALSE (always). Surface as a
        // typed unsupported error rather than throwing (per the
        // standard).
        unsupported("Predicate.In with empty values list")
      } else {
        val fieldRef = toFieldRef(field)
        // Chain via flatMap so a single bad literal short-circuits
        // the whole In() (per error-handling-style.md).
        val equalsE: Either[EngineError.UnsupportedCapability, List[Expr]] =
          values.foldLeft[Either[EngineError.UnsupportedCapability, List[Expr]]](Right(Nil)) { (acc, v) =>
            acc.flatMap(es => toLiteral(v).map(lit => es :+ Expr.Equal(fieldRef, lit)))
          }
        equalsE.map { equals =>
          // foldLeft with an explicit Expr accumulator so the inferred
          // type is Expr (not Expr.Equal), matching Expr.Or's signature.
          val orChain: Expr = equals.tail.foldLeft[Expr](equals.head)((acc, eq) => Expr.Or(acc, eq))
          if (negate) Expr.Not(orChain) else orChain
        }
      }

    case Predicate.IsNull(field, negate) =>
      val isNull = Expr.IsNull(toFieldRef(field))
      Right(if (negate) Expr.Not(isNull) else isNull)

    // -------------------------------------------------------------------------
    // Compound (recursive, fail-fast via flatMap per the standard)
    // -------------------------------------------------------------------------
    case Predicate.And(children @ _*) =>
      // Per error-handling-style.md: chain via flatMap. Children
      // is varargs (non-empty per legacy invariant); reduce
      // returns the single child unchanged if only one present.
      children.map(toExpr).reduceLeft((acc, e) => acc.flatMap(a => e.map(b => Expr.And(a, b))))
    case Predicate.Or(children @ _*) =>
      children.map(toExpr).reduceLeft((acc, e) => acc.flatMap(a => e.map(b => Expr.Or(a, b))))
    case Predicate.Not(pred) =>
      toExpr(pred).map(Expr.Not(_))
  }

  /** Bulk conversion for `Seq[Predicate]`. Convenience for callers
    * that hold collections. Short-circuits on the first failure
    * (preserves fail-fast semantics per scala-chaos-testing §2). */
  def toExprAll(ps: Seq[Predicate]): Either[EngineError.UnsupportedCapability, Seq[Expr]] =
    ps.foldLeft[Either[EngineError.UnsupportedCapability, Seq[Expr]]](Right(Seq.empty)) {
      (acc, p) => acc.flatMap(seq => toExpr(p).map(e => seq :+ e))
    }

  /** Round-trip determinism: two conversions of the same legacy
    * predicate produce the same portable Expr. Useful in tests as
    * an invariant at the boundary. */
  def roundTripEquals(a: Predicate): Boolean = {
    val portable = toExpr(a)
    toExpr(a) == portable
  }

  // -- helpers --

  private def toFieldRef(name: String): Expr = Expr.FieldRef(name)

  /** Map a legacy `Any` value to a portable `Literal`. Handles
    * the common JVM + Spark types; unknown types return
    * `Left(EngineError.UnsupportedCapability(...))`. */
  private def toLiteral(value: Any): Either[EngineError.UnsupportedCapability, Expr.Literal] = value match {
    case null => Right(Expr.Literal(LiteralValue.NullValue, SealedDataType.Varchar))
    case s: String => Right(Expr.Literal(LiteralValue.StringValue(s), SealedDataType.Varchar))
    case i: Int => Right(Expr.Literal(LiteralValue.IntValue(i), SealedDataType.Int))
    case l: Long => Right(Expr.Literal(LiteralValue.LongValue(l), SealedDataType.Int))
    case s: Short => Right(Expr.Literal(LiteralValue.ShortValue(s), SealedDataType.Int))
    case b: Byte => Right(Expr.Literal(LiteralValue.ByteValue(b), SealedDataType.Int))
    case d: Double => Right(Expr.Literal(LiteralValue.DoubleValue(d), SealedDataType.Double))
    case f: Float => Right(Expr.Literal(LiteralValue.FloatValue(f), SealedDataType.Double))
    case b: Boolean => Right(Expr.Literal(LiteralValue.BoolValue(b), SealedDataType.Boolean))
    case bd: BigDecimal => Right(Expr.Literal(LiteralValue.DecimalValue(bd), SealedDataType.Decimal(38, 18)))
    case ts: SqlTimestamp => Right(Expr.Literal(LiteralValue.TimestampValue(ts.toInstant), SealedDataType.Timestamp))
    case ts: java.time.Instant => Right(Expr.Literal(LiteralValue.TimestampValue(ts), SealedDataType.Timestamp))
    case d: SqlDate => Right(Expr.Literal(LiteralValue.DateValue(d.toLocalDate), SealedDataType.Date))
    case d: java.time.LocalDate => Right(Expr.Literal(LiteralValue.DateValue(d), SealedDataType.Date))
    case other => unsupported(s"literal of type ${other.getClass.getSimpleName}")
  }

  /** Build a typed `Left` for an unsupported legacy predicate shape.
    * Returns `Either` (not `throw`) per error-handling-style.md. */
  private def unsupported(reason: String): Left[EngineError.UnsupportedCapability, Nothing] =
    Left(EngineError.UnsupportedCapability(
      name   = reason,
      reason = s"$reason is not supported by the portable bridge (deferred to v0.4.0).",
    ))
}
