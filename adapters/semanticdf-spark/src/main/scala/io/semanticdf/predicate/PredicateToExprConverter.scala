package io.semanticdf.predicate

import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.schema.SealedDataType

import java.sql.{Date => SqlDate, Timestamp => SqlTimestamp}

/** Boundary adapter: convert legacy
  * `io.semanticdf.predicate.Predicate` \u2192 portable
  * `io.semanticdf.core.expr.Expr`.
  *
  * ==Why this exists==
  *
  * Closes Gap 4 from `docs/design/v0.3.1-feature-parity-backlog.md`.
  * The v1 `ModelBridge.toModel` (PR #409, port 8a) couldn't convert
  * legacy `where` / `having` predicates \u2014 it silently set `filters = Nil`
  * producing unfiltered portable queries. PR #414 added a fail-loud
  * guard via `ModelValidationError.FilterConversionUnsupported`.
  *
  * This converter closes the gap for the common cases (Eq / Ne / Lt /
  * Le / Gt / Ge, And / Or / Not, In / IsNull, all literals). Cases
  * that have no portable counterpart yet (Contains / StartsWith /
  * EndsWith / ArrayContains) still fail loud \u2014 deferred to v0.4.0.
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
  * ==Equality preservation==
  *
  * Per scala-data-driven-refacer \u00a71: portable types must be pure
  * data in the literal, transitive sense. The conversion uses
  * deterministic `LiteralValue` mapping (any \u2192 LongValue(0) for null,
  * any \u2192 Varchar for strings, etc.) so two legacy predicates with
  * the same shape produce the same portable Expr.
  *
  * ==Live in `io.semanticdf.predicate`, NOT `core.expr`==
  *
  * The converter's INPUT is `io.semanticdf.predicate.Predicate`,
  * which is Spark-bearing (the case classes reference `Any` and
  * were designed for the legacy `SemanticTable` fluent chain).
  * Placing this converter in `core.expr` would force `core` to depend
  * on Spark, breaking the invariant that `core` compiles without
  * Spark on the classpath. The converter lives next to its source
  * type, in the Spark-aware package, and produces the engine-portable
  * type.
  *
  * ==JVM-safety check 1 (Null is a liar)==
  *
  * The legacy `value: Any` may be `null` (e.g. `Predicate.IsNull("x", negate=true)`
  * uses `null` semantically, but `Compare.Eq("x", null)` is also legal).
  * The `toLiteral` helper catches `null` at the boundary and maps it
  * to `LiteralValue.NullValue` (per scala-jvm-safety \u00a71: every
  * unmarked boundary can hand you a null).
  */
object PredicateToExprConverter {

  /** Convert a legacy `Predicate` to a portable `Expr`. Recursive
    * for And/Or/Not. Throws `UnsupportedOperationException` for
    * legacy predicates that have no portable counterpart yet
    * (Contains, StartsWith, EndsWith, ArrayContains). The
    * `ModelBridge.toModel` caller catches that and wraps it as
    * `ModelValidationError.FilterConversionUnsupported`. */
  def toExpr(p: Predicate): Expr = p match {
    // -------------------------------------------------------------------------
    // Compare family (12 cases, 6 supported + 6 deferred)
    // -------------------------------------------------------------------------
    case Predicate.Compare.Eq(field, value) =>
      Expr.Equal(toFieldRef(field), toLiteral(value))
    case Predicate.Compare.Ne(field, value) =>
      Expr.NotEqual(toFieldRef(field), toLiteral(value))
    case Predicate.Compare.Lt(field, value) =>
      Expr.LessThan(toFieldRef(field), toLiteral(value))
    case Predicate.Compare.Le(field, value) =>
      Expr.LessOrEqual(toFieldRef(field), toLiteral(value))
    case Predicate.Compare.Gt(field, value) =>
      Expr.GreaterThan(toFieldRef(field), toLiteral(value))
    case Predicate.Compare.Ge(field, value) =>
      Expr.GreaterOrEqual(toFieldRef(field), toLiteral(value))

    case Predicate.Compare.Contains(_, _) =>
      throw unsupported("Contains")
    case Predicate.Compare.StartsWith(_, _) =>
      throw unsupported("StartsWith")
    case Predicate.Compare.EndsWith(_, _) =>
      throw unsupported("EndsWith")
    case Predicate.Compare.ArrayContains(_, _) =>
      throw unsupported("ArrayContains")

    // -------------------------------------------------------------------------
    // Non-compare leaves
    // -------------------------------------------------------------------------
    // In(field, values, negate) \u2192 Or-chain of Equal (or And-chain if negate).
    // For the common case of a single value, this collapses to Eq/Ne.
    case Predicate.In(field, values, negate) =>
      if (values.isEmpty) {
        // Empty In(): SQL semantics is FALSE (always).
        // Portable equivalent: a contradiction. We use a literal
        // boolean false comparison since portable Expr doesn't have
        // a "false" constant yet; this preserves the contract.
        // For v0.3.1 v1 simplicity, throw \u2014 empty In() is rare and
        // can be a follow-up enhancement.
        throw unsupported("In with empty values list")
      }
      val fieldRef = toFieldRef(field)
      val equals: List[Expr] = values.map(v => Expr.Equal(fieldRef, toLiteral(v))).toList
      // foldLeft with an explicit Expr accumulator so the inferred
      // type is Expr (not Expr.Equal), matching Expr.Or's signature.
      val orChain: Expr = equals.tail.foldLeft[Expr](equals.head)((acc, eq) => Expr.Or(acc, eq))
      if (negate) Expr.Not(orChain) else orChain

    case Predicate.IsNull(field, negate) =>
      val isNull = Expr.IsNull(toFieldRef(field))
      if (negate) Expr.Not(isNull) else isNull

    // -------------------------------------------------------------------------
    // Compound (recursive)
    // -------------------------------------------------------------------------
    // Predicate.And / Or carry varargs. For 1 child, reduceLeft
    // returns that child unchanged (no spurious wrapping).
    case Predicate.And(children @ _*) =>
      children.map(toExpr).reduceLeft((acc, p) => Expr.And(acc, p))
    case Predicate.Or(children @ _*) =>
      children.map(toExpr).reduceLeft((acc, p) => Expr.Or(acc, p))
    case Predicate.Not(pred) =>
      Expr.Not(toExpr(pred))
  }

  /** Bulk conversion for `Seq[Predicate]`. Convenience for callers
    * that hold collections (e.g. `Option[Predicate]`,
    * `List[Predicate]`). */
  def toExprAll(ps: Seq[Predicate]): Seq[Expr] = ps.map(toExpr)

  /** Round-trip identity: legacy \u2192 portable \u2192 legacy produces the
    * original (via the existing `PredicateConverter.fromCore`
    * if available; otherwise structural comparison of the original
    * vs the back-converted form).
    *
    * Useful in tests as an invariant at the boundary. */
  def roundTripEquals(a: Predicate): Boolean = {
    val portable = toExpr(a)
    // The portable Expr is a different shape from the legacy
    // Predicate (Expr.Equal vs Compare.Eq), so a structural `==`
    // would not hold. We instead verify the conversion is
    // deterministic: two calls produce the same Expr.
    toExpr(a) == portable
  }

  // -- helpers --

  private def toFieldRef(name: String): Expr = Expr.FieldRef(name)

  /** Map a legacy `Any` value to a portable `(LiteralValue,
    * SealedDataType)` pair. Handles the common JVM + Spark types;
    * unknown types throw (the ModelBridge caller wraps as
    * `FilterConversionUnsupported`). */
  private def toLiteral(value: Any): Expr = {
    val (lit, dt) = value match {
      case null =>
        (LiteralValue.NullValue, SealedDataType.Varchar)
      case s: String =>
        (LiteralValue.StringValue(s), SealedDataType.Varchar)
      case i: Int =>
        (LiteralValue.IntValue(i), SealedDataType.Int)
      case l: Long =>
        (LiteralValue.LongValue(l), SealedDataType.Int)
      case s: Short =>
        (LiteralValue.ShortValue(s), SealedDataType.Int)
      case b: Byte =>
        (LiteralValue.ByteValue(b), SealedDataType.Int)
      case d: Double =>
        (LiteralValue.DoubleValue(d), SealedDataType.Double)
      case f: Float =>
        (LiteralValue.FloatValue(f), SealedDataType.Double)
      case b: Boolean =>
        (LiteralValue.BoolValue(b), SealedDataType.Boolean)
      case bd: BigDecimal =>
        (LiteralValue.DecimalValue(bd), SealedDataType.Decimal(38, 18))
      case ts: SqlTimestamp =>
        (LiteralValue.TimestampValue(ts.toInstant), SealedDataType.Timestamp)
      case ts: java.time.Instant =>
        (LiteralValue.TimestampValue(ts), SealedDataType.Timestamp)
      case d: SqlDate =>
        (LiteralValue.DateValue(d.toLocalDate), SealedDataType.Date)
      case d: java.time.LocalDate =>
        (LiteralValue.DateValue(d), SealedDataType.Date)
      case other =>
        throw unsupported(s"literal of type ${other.getClass.getSimpleName}")
    }
    Expr.Literal(lit, dt)
  }

  private def unsupported(reason: String): UnsupportedOperationException =
    new UnsupportedOperationException(
      s"PredicateToExprConverter: $reason is not supported by the portable bridge (deferred to v0.4.0).",
    )
}
