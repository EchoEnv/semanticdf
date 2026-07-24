package io.semanticdf.mcp.handlers

import io.semanticdf.Predicate

import scala.jdk.CollectionConverters._

/** JSON predicate adapter for the **structured AST** wire format.
  *
  * This is the alternative predicate shape exposed on the `query` tool's
  * `ast_where` (and `ast_having`) field. It is intentionally narrower than
  * [[JsonPredicates]] — only the ops the library's [[PredicateAst]] model
  * actually produces (`eq` / `neq` / `lt` / `lte` / `gt` / `gte` /
  * `and` / `or`) — so agents authoring a predicate AST have a closed
  * surface to target. The flat `where`/`having` shape stays as the
  * ergonomic alternative; this one is for agents that want to express
  * composable nested predicates directly.
  *
  * Wire format:
  *
  *   leaf:      `{"op": "eq",  "left": "carrier",  "right": "AA"}`
  *   numeric:   `{"op": "gt",  "left": "distance", "right": 500}`
  *   boolean:   `{"op": "gt",  "left": "active",   "right": true}`
  *   compound:  `{"op": "and",  "left": <node>,    "right": <node>}`
  *   compound:  `{"op": "or",   "left": <node>,    "right": <node>}`
  *
  * Detection rule: a JSON object that has an `op` key → a node; anything
  * else is a literal value (string / int / double / boolean). This lets
  * the leaf shape stay flat — no nested `{type: "col", ...}` wrapper.
  *
  * Errors:
  *   - `INVALID_PREDICATE` — missing `op` (for nodes), missing `left` /
  *     `right` keys, bad shape (e.g. `left` is a number when an op needs
  *     a node).
  *   - `UNSUPPORTED_OP`     — `op` value not in the closed list above.
  *
  * The errors share the same exception types as [[JsonPredicates]] so the
  * SDK adapter can map both to the same MCP error envelopes. */
object AstPredicates {

  /** Closed op set. Matches the library's [[PredicateAst.Op]] names exactly
    * so the AST and the library's wire shape stay in lockstep. */
  private val Ops: Map[String, String] = Map(
    "eq"  -> "eq",  "neq" -> "neq",
    "lt"  -> "lt",  "lte" -> "lte",
    "gt"  -> "gt",  "gte" -> "gte",
    "and" -> "and", "or"  -> "or",
  )

  /** Convert one AST node (or an array containing one node) into a
    * library [[Predicate]]. Throws on shape errors and unknown ops. */
  def parse(json: Any): Predicate = parseNode(json)

  private def parseNode(json: Any): Predicate = {
    val map: Map[String, Any] = json match {
      case m: java.util.Map[_, _] => m.asScala.toMap.asInstanceOf[Map[String, Any]]
      case m: Map[_, _]          => m.asInstanceOf[Map[String, Any]]
      case other => throw JsonPredicates.InvalidPredicate(
        s"AST node must be a JSON object, got ${other.getClass.getSimpleName}")
    }
    val op = map.get("op") match {
      case Some(s: String) if Ops.contains(s) => s
      case Some(s: String) => throw JsonPredicates.UnsupportedOp(s)
      case _ => throw JsonPredicates.InvalidPredicate(
        s"AST node missing 'op' key (allowed: ${Ops.keys.toSeq.sorted.mkString(", ")})")
    }
    if (!map.contains("left") || !map.contains("right"))
      throw JsonPredicates.InvalidPredicate(
        s"AST node 'op=$op' must have both 'left' and 'right' keys")
    val left  = map("left")
    val right = map("right")
    op match {
      case "and" => Predicate.And(parseNode(left), parseNode(right))
      case "or"  => Predicate.Or(parseNode(left),  parseNode(right))
      // Compare ops: left must be a field name (string), right is a value.
      // Map the AST names (neq/lte/gte) to the library's Predicate.Compare
      // short codes (ne/le/ge) — the library uses 2-letter codes.
      case cmp =>
        val libOp = cmp match {
          case "neq" => "ne"
          case "lte" => "le"
          case "gte" => "ge"
          case other => other
        }
        val field = left match {
          case s: String => s
          case other => throw JsonPredicates.InvalidPredicate(
            s"AST compare 'op=$cmp' requires 'left' to be a field name (string), got ${other.getClass.getSimpleName}")
        }
        Predicate.Compare(libOp, field, right)
    }
  }
}
