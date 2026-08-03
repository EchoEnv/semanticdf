package io.semanticdf.mcp.handlers

import io.semanticdf.core.predicate.{Predicate => CorePredicate}
import io.semanticdf.predicate.Predicate
import io.semanticdf.predicate.Predicate.Compare

import scala.jdk.CollectionConverters._

/** JSON predicate adapter — translates the JSON shape documented in
  * `docs/agents/mcp-contract.md` v2 §"Tool 3: query" into the library's
  * `Predicate` AST.
  *
  * Allowed `type` values:
  *
  *   Compare:   `eq` `ne` `lt` `le` `gt` `ge`
  *   In:        `in` `not_in`
  *   IsNull:    `is_null` `is_not_null`
  *   Compound:  `and` (`predicates`), `or` (`predicates`), `not` (`predicate`)
  *
  * Field `field` is a required string; `value` (Compare), `values` (In) are
  * typed by the consumer — we accept `Object` here and let Spark's compiled
  * predicates handle the type coercion at runtime.
  *
  * Errors:
  *   - `INVALID_PREDICATE` — missing `type`, missing `field`, bad compound,
  *     wrong shape (`predicates` not an array, `predicate` not an object).
  *   - `UNSUPPORTED_OP`     — `type` value not in the closed list above.
  *
  * Both errors are raised as exceptions so the SDK adapter can map them to
  * closed-list error envelopes.
  *
  * ==Two public entry points==
  *
  *   - [[parse]] / [[parseAll]] — produce the Spark-bearing
  *     `io.semanticdf.predicate.Predicate`. Used by `SemanticTable.query`'s
  *     fluent API (the user-facing surface) and by the existing test suite.
  *   - [[parseCore]] / [[parseAllCore]] — produce the engine-portable
  *     `io.semanticdf.core.predicate.Predicate` directly. Used by the audit /
  *     cache-key chain and any future wire-format encoders. The core ADT
  *     compiles without Spark on the classpath, so JSON adapters producing
  *     core predicates keep that property intact.
  *
  * Both entry points validate the same JSON shape and throw the same error
  * types — they differ only in which ADT they construct. */
object JsonPredicates {

  // -------------------------------------------------------------------------
  // Public entry — dispatch on `type`
  // -------------------------------------------------------------------------

  /** Convert one JSON predicate (a `java.util.Map[String, Any]`) into the
    * library's `Predicate` AST. Throws `InvalidPredicate` on shape errors and
    * `UnsupportedOp` on unknown op names. We always expect Java maps (the
    * MCP SDK's `Map[String, Object]` is a Java map under the hood).
    *
    * Kept as the legacy public API — the fluent `SemanticTable.query` surface
    * is typed as `io.semanticdf.predicate.Predicate`, so this entry point is
    * the bridge for callers that need to flow directly into the library API
    * without an intermediate core conversion. */
  def parse(json: Any): Predicate = {
    val map = json match {
      case m: java.util.Map[_, _] => m.asScala.toMap.asInstanceOf[Map[String, Any]]
      case other                 => throw InvalidPredicate(s"predicate must be a JSON object, got ${other.getClass.getSimpleName}")
    }
    parseMap(map)
  }

  /** Convert a `List[Any]` of JSON predicates into one `Predicate` by
    * AND-combining them with [[Predicate.And]]. An empty list returns
    * None (the contract specifies "the agent never writes `And`
    * wrappers manually"; the server produces one when needed). */
  def parseAll(jsonList: Seq[Any]): Option[Predicate] = jsonList match {
    case Nil      => None
    case one :: Nil => Some(parse(one))
    case many     => Some(Predicate.And(many.map(parse): _*))
  }

  // -------------------------------------------------------------------------
  // Core entry — engine-portable ADT
  // -------------------------------------------------------------------------

  /** Convert one JSON predicate into the engine-portable
    * `io.semanticdf.core.predicate.Predicate` AST. Throws the same
    * [[InvalidPredicate]] / [[UnsupportedOp]] errors as [[parse]] — the
    * JSON shape contract is identical.
    *
    * Use this when the caller already operates on the core ADT (e.g. the
    * audit / cache-key chain) or wants to keep the result engine-portable
    * (e.g. future wire-format encoders that target Trino / Databricks). */
  def parseCore(json: Any): CorePredicate = {
    val map = json match {
      case m: java.util.Map[_, _] => m.asScala.toMap.asInstanceOf[Map[String, Any]]
      case other                 => throw InvalidPredicate(s"predicate must be a JSON object, got ${other.getClass.getSimpleName}")
    }
    parseMapCore(map)
  }

  /** Batch variant of [[parseCore]]. AND-combines multiple predicates with
    * [[io.semanticdf.core.predicate.Predicate.And]]. Empty input → `None`. */
  def parseAllCore(jsonList: Seq[Any]): Option[CorePredicate] = jsonList match {
    case Nil      => None
    case one :: Nil => Some(parseCore(one))
    case many     => Some(CorePredicate.And(many.map(parseCore): _*))
  }

  // -------------------------------------------------------------------------
  // Inner dispatcher — `type` key
  // -------------------------------------------------------------------------

  /** Eta-expanded bindings: each Compare subtype's apply has type
    * `(String, Any) => Compare.<sub>`, which is a subtype of
    * `(String, Any) => Predicate` via covariance. The explicit
    * `Predicate.` annotation widens the type so pattern-match dispatch
    * works without polymorphic-expression errors. */
  private val CmpEq: (String, Any) => Predicate = Compare.Eq
  private val CmpNe: (String, Any) => Predicate = Compare.Ne
  private val CmpLt: (String, Any) => Predicate = Compare.Lt
  private val CmpLe: (String, Any) => Predicate = Compare.Le
  private val CmpGt: (String, Any) => Predicate = Compare.Gt
  private val CmpGe: (String, Any) => Predicate = Compare.Ge

  // Core ADT equivalents — same eta-expansion trick, target the core ADT.
  private val CmpEqCore: (String, Any) => CorePredicate = io.semanticdf.core.predicate.Predicate.Compare.Eq
  private val CmpNeCore: (String, Any) => CorePredicate = io.semanticdf.core.predicate.Predicate.Compare.Ne
  private val CmpLtCore: (String, Any) => CorePredicate = io.semanticdf.core.predicate.Predicate.Compare.Lt
  private val CmpLeCore: (String, Any) => CorePredicate = io.semanticdf.core.predicate.Predicate.Compare.Le
  private val CmpGtCore: (String, Any) => CorePredicate = io.semanticdf.core.predicate.Predicate.Compare.Gt
  private val CmpGeCore: (String, Any) => CorePredicate = io.semanticdf.core.predicate.Predicate.Compare.Ge

  private def parseMap(map: Map[String, Any]): Predicate = {
    val opt = map.get("type")
    if (opt.isEmpty) throw InvalidPredicate(s"predicate missing required 'type' field: ${map.keys.mkString(", ")}")
    val op = opt.get match {
      case s: String => s
      case other     => throw InvalidPredicate(s"predicate 'type' must be a string, got ${other.getClass.getSimpleName}")
    }
    op match {
      case "eq" => compare(map, CmpEq)
      case "ne" => compare(map, CmpNe)
      case "lt" => compare(map, CmpLt)
      case "le" => compare(map, CmpLe)
      case "gt" => compare(map, CmpGt)
      case "ge" => compare(map, CmpGe)
      case "in"         => inOp(map, negate = false)
      case "not_in"     => inOp(map, negate = true)
      case "is_null"    => isNullOp(map, negate = false)
      case "is_not_null"=> isNullOp(map, negate = true)
      case "and"        => compound(map, ps => Predicate.And(ps: _*))
      case "or"         => compound(map, ps => Predicate.Or(ps: _*))
      case "not"        => not(map)
      case other        => throw UnsupportedOp(other)
    }
  }

  // Core ADT dispatcher — same logic as `parseMap`, but routes to the
  // core ADT factories. Keeps the JSON shape contract identical (same
  // error types, same validation).
  private def parseMapCore(map: Map[String, Any]): CorePredicate = {
    val opt = map.get("type")
    if (opt.isEmpty) throw InvalidPredicate(s"predicate missing required 'type' field: ${map.keys.mkString(", ")}")
    val op = opt.get match {
      case s: String => s
      case other     => throw InvalidPredicate(s"predicate 'type' must be a string, got ${other.getClass.getSimpleName}")
    }
    op match {
      case "eq" => compareCore(map, CmpEqCore)
      case "ne" => compareCore(map, CmpNeCore)
      case "lt" => compareCore(map, CmpLtCore)
      case "le" => compareCore(map, CmpLeCore)
      case "gt" => compareCore(map, CmpGtCore)
      case "ge" => compareCore(map, CmpGeCore)
      case "in"          => inOpCore(map, negate = false)
      case "not_in"      => inOpCore(map, negate = true)
      case "is_null"     => isNullOpCore(map, negate = false)
      case "is_not_null" => isNullOpCore(map, negate = true)
      case "and"         => compoundCore(map, ps => CorePredicate.And(ps: _*))
      case "or"          => compoundCore(map, ps => CorePredicate.Or(ps: _*))
      case "not"         => notCore(map)
      case other         => throw UnsupportedOp(other)
    }
  }

  // -------------------------------------------------------------------------
  // Leaf-shape parsers
  // -------------------------------------------------------------------------

  private def compare(map: Map[String, Any], ctor: (String, Any) => Predicate): Predicate = {
    val field = requireField(map)
    val value = map.get("value") match {
      case Some(v) => v
      case None    => throw InvalidPredicate(s"${ctor("X", null).getClass.getSimpleName}: 'value' is required")
    }
    ctor(field, value)
  }

  private def inOp(map: Map[String, Any], negate: Boolean): Predicate = {
    val field = requireField(map)
    val values = map.get("values") match {
      case Some(s: Seq[_]) => s
      case Some(arr: Array[_]) => arr.toSeq
      case Some(juList: java.util.List[_]) => juList.asScala.toSeq
      case None => throw InvalidPredicate(s"${if (negate) "not_in" else "in"}: 'values' array is required")
      case other => throw InvalidPredicate(s"${if (negate) "not_in" else "in"}: 'values' must be an array, got ${other.getClass.getSimpleName}")
    }
    if (values.isEmpty) throw InvalidPredicate(s"${if (negate) "not_in" else "in"}: 'values' must not be empty")
    Predicate.In(field, values, negate = negate)
  }

  private def isNullOp(map: Map[String, Any], negate: Boolean): Predicate = {
    val field = requireField(map)
    Predicate.IsNull(field, negate = negate)
  }

  private def compound(map: Map[String, Any], ctor: Seq[Predicate] => Predicate): Predicate = {
    val children = map.get("predicates") match {
      case Some(s: Seq[_]) => s.toList
      case Some(juList: java.util.List[_]) => juList.asScala.toList
      case None => throw InvalidPredicate(s"compound: 'predicates' array is required")
      case other => throw InvalidPredicate(s"compound: 'predicates' must be an array, got ${other.getClass.getSimpleName}")
    }
    if (children.length < 2)
      throw InvalidPredicate(s"compound: 'predicates' must contain at least 2 elements (got ${children.length})")
    ctor(children.map(parse))
  }

  private def not(map: Map[String, Any]): Predicate = {
    val inner = map.get("predicate") match {
      case None =>
        throw InvalidPredicate("not: a nested 'predicate' object is required")
      case Some(p) => parse(p)
    }
    Predicate.Not(inner)
  }

  // -------------------------------------------------------------------------
  // Core ADT leaf parsers (mirror of the above)
  // -------------------------------------------------------------------------

  private def compareCore(map: Map[String, Any], ctor: (String, Any) => CorePredicate): CorePredicate = {
    val field = requireField(map)
    val value = map.get("value") match {
      case Some(v) => v
      case None    => throw InvalidPredicate(s"${ctor("X", null).getClass.getSimpleName}: 'value' is required")
    }
    ctor(field, value)
  }

  private def inOpCore(map: Map[String, Any], negate: Boolean): CorePredicate = {
    val field = requireField(map)
    val values = map.get("values") match {
      case Some(s: Seq[_]) => s
      case Some(arr: Array[_]) => arr.toSeq
      case Some(juList: java.util.List[_]) => juList.asScala.toSeq
      case None => throw InvalidPredicate(s"${if (negate) "not_in" else "in"}: 'values' array is required")
      case other => throw InvalidPredicate(s"${if (negate) "not_in" else "in"}: 'values' must be an array, got ${other.getClass.getSimpleName}")
    }
    if (values.isEmpty) throw InvalidPredicate(s"${if (negate) "not_in" else "in"}: 'values' must not be empty")
    CorePredicate.In(field, values, negate = negate)
  }

  private def isNullOpCore(map: Map[String, Any], negate: Boolean): CorePredicate = {
    val field = requireField(map)
    CorePredicate.IsNull(field, negate = negate)
  }

  private def compoundCore(map: Map[String, Any], ctor: Seq[CorePredicate] => CorePredicate): CorePredicate = {
    val children = map.get("predicates") match {
      case Some(s: Seq[_]) => s.toList
      case Some(juList: java.util.List[_]) => juList.asScala.toList
      case None => throw InvalidPredicate("compound: 'predicates' array is required")
      case other => throw InvalidPredicate(s"compound: 'predicates' must be an array, got ${other.getClass.getSimpleName}")
    }
    if (children.length < 2)
      throw InvalidPredicate(s"compound: 'predicates' must contain at least 2 elements (got ${children.length})")
    ctor(children.map(parseCore))
  }

  private def notCore(map: Map[String, Any]): CorePredicate = {
    val inner = map.get("predicate") match {
      case None =>
        throw InvalidPredicate("not: a nested 'predicate' object is required")
      case Some(p) => parseCore(p)
    }
    CorePredicate.Not(inner)
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def requireField(map: Map[String, Any]): String = map.get("field") match {
    case Some(s: String) => s
    case None             => throw InvalidPredicate("predicate missing required 'field' (string)")
    case other            => throw InvalidPredicate(s"'field' must be a string, got ${other.getClass.getSimpleName}")
  }

  /** Adapter errors. The SDK adapter catches these by type. */
  final case class InvalidPredicate(message: String)
      extends RuntimeException(s"INVALID_PREDICATE: $message")

  final case class UnsupportedOp(op: String)
      extends RuntimeException(s"UNSUPPORTED_OP: '$op'")
}
