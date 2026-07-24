package io.semanticdf

import scala.jdk.CollectionConverters._

/** Cross-version AST walker for the join predicate. Used by the
  * `PredicateAst` round-trip path (v0.1.13).
  *
  * Performance / lifecycle:
  *   - Only called when the caller needs the structured form. The
  *     equi-join fast path uses `leftKeys` / `rightKeys` alone and
  *     skips the AST capture entirely.
  *   - Uses reflection to walk Spark 3.x `Expression`s and Spark 4.x
  *     `ColumnNode`s without direct package dependencies.
  *   - No allocations beyond what the existing probe path already
  *     does for `walkJoinKeyAst`. The probe caches are reused.
  */
private[semanticdf] object PredicateAstWalker {

  /** Walks the join predicate Column tree and returns a structured
    * [[PredicateAst.Predicate]] when the structure fits the library's
    * vocabulary. `None` if the structure doesn't fit (UDFs, etc.) or
    * the probe failed. */
  def walkPredicateAst(
      node: Any,
      leftCapture: scala.collection.mutable.Map[String, Boolean],
      rightCapture: scala.collection.mutable.Map[String, Boolean],
  ): Option[PredicateAst.Operand] = {
    val untag = (s: String) =>
      if (s.startsWith("__left__"))  s.stripPrefix("__left__")
      else if (s.startsWith("__right__")) s.stripPrefix("__right__")
      else s
    def sqlOf(n: Any): String = if (n == null) "" else
      try n.getClass.getMethod("sql").invoke(n).asInstanceOf[String]
      catch { case _: NoSuchMethodException => "" }
    def fnNameOf(n: Any): String = if (n == null) "" else
      try n.getClass.getMethod("functionName").invoke(n).asInstanceOf[String]
      catch { case _: NoSuchMethodException => "" }
    def childrenOf(n: Any): Seq[Any] = if (n == null) Seq.empty else
      try {
        n.getClass.getMethod("children").invoke(n) match {
          case s: Seq[_]        => s.asInstanceOf[Seq[Any]]
          case l: java.util.List[_] =>
            val out = scala.collection.mutable.ListBuffer.empty[Any]
            l.forEach(out += _)
            out.toSeq
          case other            => Seq(other)
        }
      } catch { case _: NoSuchMethodException => Seq.empty }

    def resolveOperand(leaf: Any): Option[PredicateAst.Operand.ColumnRef] = {
      val s = sqlOf(leaf)
      if (leftCapture.contains(s))
        Some(PredicateAst.Operand.ColumnRef("left", untag(s)))
      else if (rightCapture.contains(s))
        Some(PredicateAst.Operand.ColumnRef("right", untag(s)))
      else None
    }
    def opFor(name: String, cls: String): Option[PredicateAst.Op] = {
      // Class-name fallback when functionName returns "" (Spark 3.x).
      val byCls: Option[PredicateAst.Op] =
        if (cls.endsWith("EqualTo") || cls.endsWith("EqualNullSafe")) Some(PredicateAst.Op.Eq)
        else if (cls.endsWith("LessThan"))    Some(PredicateAst.Op.Lt)
        else if (cls.endsWith("LessThanOrEqual"))    Some(PredicateAst.Op.Lte)
        else if (cls.endsWith("GreaterThan")) Some(PredicateAst.Op.Gt)
        else if (cls.endsWith("GreaterThanOrEqual")) Some(PredicateAst.Op.Gte)
        else None
      if (byCls.isDefined) return byCls
      name match {
        case "="  | "==="       => Some(PredicateAst.Op.Eq)
        case "!"  | "<>" | "!=" => Some(PredicateAst.Op.Neq)
        case "<"               => Some(PredicateAst.Op.Lt)
        case "<="              => Some(PredicateAst.Op.Lte)
        case ">"               => Some(PredicateAst.Op.Gt)
        case ">="              => Some(PredicateAst.Op.Gte)
        case "&&" | "and"      => Some(PredicateAst.Op.And)
        case "||" | "or"       => Some(PredicateAst.Op.Or)
        case _                 => None
      }
    }
    def walk(n: Any): Option[PredicateAst.Operand] = {
      if (n == null) return None
      val fname = fnNameOf(n)
      val cls   = n.getClass.getName
      // For binary comparisons the class name is the most reliable
      // signal across Spark versions: Spark 3.x uses EqualTo / Lt /
      // etc. (functionName returns ""), Spark 4.x may use
      // UnresolvedFunction("=") where functionName returns "=". Either
      // path works - we accept either condition.
      val isBinary = fname.nonEmpty || cls.endsWith("EqualTo") || cls.endsWith("EqualNullSafe") ||
        cls.endsWith("LessThan") || cls.endsWith("LessThanOrEqual") ||
        cls.endsWith("GreaterThan") || cls.endsWith("GreaterThanOrEqual")
      val isAnd = fname == "&&" || fname == "and" || cls.endsWith("And")
      val isOr  = fname == "||" || fname == "or"  || cls.endsWith("Or")
      if (isBinary || isAnd || isOr) {
        val kids = childrenOf(n)
        if (kids.length == 2) {
          val op = if (isBinary) opFor(fname, cls)
                  else if (isAnd) Some(PredicateAst.Op.And)
                  else Some(PredicateAst.Op.Or)
          (op, walk(kids(0)), walk(kids(1))) match {
            case (Some(o), Some(l), Some(r)) => Some(PredicateAst.Predicate(o, l, r))
            case _ => None
          }
        } else if (kids.length == 1 && (isAnd || isOr)) walk(kids(0))
        else None
      } else resolveOperand(n)
    }
    try walk(node) catch { case _: Throwable => None }
  }
}
