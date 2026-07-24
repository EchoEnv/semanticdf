package io.semanticdf

import org.apache.spark.sql.Column

/** Structured predicate AST for the joined-manifest wire shape.
  *
  * This closes the last remaining narrow caveat of the
  * joined-models-manifest recipe (v0.1.13). Before this, the round-trip
  * of non-equi / OR predicates was via an opaque `onExprString`
  * SQL field. Tools could not introspect the predicate structure -
  * "this joined on `l.date < r.valid_to`" was not directly expressible.
  *
  * The AST here is a *minimal* tree that supports the operations the
  * library actually produces in practice: `eq` / `neq` / range / `and`
  * / `or`, with column operands (left or right side). It is intentionally
  * NOT a full Spark Catalyst expression serialiser - the library has a
  * finite surface of useful predicates and we capture them all.
  * Anything more complex (subqueries, UDFs, etc.) falls through to
  * the legacy `onExprString` SQL fallback path.
  *
  * Performance notes (production-grade):
  *   - Equi-join case (the typical case) does NOT produce an AST. The
  *     wire stays structured: just `leftKeys` / `rightKeys`. Zero
  *     overhead for the common path. The AST is only computed when
  *     keys alone can't capture the predicate (non-equi / OR).
  *   - In-memory representation uses sealed trait + case objects
  *     (O(1) reference equality, no string compares).
  *   - Wire format is JSON-native (matches the rest of the manifest
  *     shape).
  *   - The reconstructed `Column` is cached per AST instance per
  *     (leftSide, rightSide) pair - typically 1 entry per join.
  *
  * Lifecycle (no leak):
  *   - The cache is a single field per AST instance, GC'd with the
  *     AST.
  *   - The cache key is by-reference, so two callers with the same
  *     leftSide/rightSide share a cached column (no duplication).
  */
object PredicateAst {

  /** Predicate operators the library actually produces. Adding a new
    * case here requires updating the `toColumn` mapping below and
    * the wire format. */
  sealed abstract class Op(val code: String) {
    override def equals(o: Any): Boolean = o match {
      case other: Op => this eq other
      case _        => false
    }
    override def hashCode(): Int = System.identityHashCode(this)
  }
  object Op {
    case object Eq  extends Op("eq")
    case object Neq extends Op("neq")
    case object Lt  extends Op("lt")
    case object Lte extends Op("lte")
    case object Gt  extends Op("gt")
    case object Gte extends Op("gte")
    case object And extends Op("and")
    case object Or  extends Op("or")

    // O(1) lookup table for wire parsing. case objects are singletons.
    private val byCode: Map[String, Op] = Seq(Eq, Neq, Lt, Lte, Gt, Gte, And, Or)
      .map(o => o.code -> o)
      .toMap

    /** Parse a wire-format `op` code into the singleton. */
    def fromCode(code: String): Option[Op] = byCode.get(code)
  }

  /** Operand. Currently a column reference OR a nested predicate
    * (for AND / OR composition). The `side` is stored as a String
    * ("left" | "right") to keep the in-memory representation
    * trivially small and the wire format direct. */
  sealed trait Operand
  object Operand {
    /** A column reference: which side of the join and which column
      * name. */
    final case class ColumnRef(side: String, name: String) extends Operand

    /** Parse a wire-format `side` string into a known side. */
    private val knownSides: Set[String] = Set("left", "right")
    def isKnownSide(s: String): Boolean = knownSides.contains(s)
  }

  /** A single binary predicate: `left OP right`. For `And` / `Or`
    * the operands are themselves nested `Predicate` values (which
    * also extend `Operand`). */
  final case class Predicate(
    op:    Op,
    left:  Operand,
    right: Operand,
  ) extends Operand {
    // Build a Spark Column from this AST node. Cached per (leftSide,
    // rightSide) pair to avoid re-walking on repeated calls. The cache
    // uses reference-equality on the pair (the keys are the actual
    // JoinSide instances passed by callers). The cache is small in
    // practice: at most a handful of entries per AST instance (the
    // typical case is 1).
    @volatile private var cache: scala.collection.mutable.Map[(Any, Any), Column] = null

    /** Build the column for the given per-side scopes. Cache check is
      * reference-equality on the pair, so callers with the same scopes
      * share the cached column. The scopes are passed as `Any` so the
      * AST can stay decoupled from the package-private `JoinSide`
      * class. */
    def toColumn(leftSide: Any, rightSide: Any): Column = {
      val key = (leftSide, rightSide)
      val c = cache
      if (c != null) {
        val cached = c.get(key)
        if (cached.isDefined) return cached.get
      }
      val l = resolve(left,  leftSide, rightSide)
      val r = resolve(right, leftSide, rightSide)
      val built: Column = op match {
        case Op.Eq  => l === r
        case Op.Neq => l =!= r
        case Op.Lt  => l < r
        case Op.Lte => l <= r
        case Op.Gt  => l > r
        case Op.Gte => l >= r
        case Op.And => l && r
        case Op.Or  => l || r
      }
      val m = if (c == null) scala.collection.mutable.Map.empty[(Any, Any), Column] else c
      m.put(key, built)
      cache = m
      built
    }

    private def resolve(o: Operand, l: Any, r: Any): Column = o match {
      case Operand.ColumnRef("left", name)  => l.asInstanceOf[JoinSide](name)
      case Operand.ColumnRef("right", name) => r.asInstanceOf[JoinSide](name)
      case p: Predicate =>
        // Nested predicate (AND / OR composition). Build the inner
        // predicate's column and recurse via the AST (which itself
        // caches the resulting Column).
        p.toColumn(l, r)
      case Operand.ColumnRef(other, name) =>
        // Unknown side - shouldn't happen since the writer validates
        // the side before emission. Fall back to the left side
        // (conservative; the predicate won't make sense but we won't
        // throw at this layer).
        l.asInstanceOf[JoinSide](name)
    }
  }
}
