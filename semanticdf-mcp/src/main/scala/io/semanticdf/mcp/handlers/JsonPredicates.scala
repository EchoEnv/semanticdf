package io.semanticdf.mcp.handlers

import io.semanticdf.Predicate
import io.semanticdf.Predicate.Compare

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
  * closed-list error envelopes. */
object JsonPredicates {

  // -------------------------------------------------------------------------
  // Public entry — dispatch on `type`
  // -------------------------------------------------------------------------

  /** Convert one JSON predicate (a `java.util.Map[String, Any]`) into the
    * library's `Predicate` AST. Throws `InvalidPredicate` on shape errors and
    * `UnsupportedOp` on unknown op names. We always expect Java maps (the
    * MCP SDK's `Map[String, Object]` is a Java map under the hood). */
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
