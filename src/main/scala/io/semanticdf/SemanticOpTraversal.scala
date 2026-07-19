package io.semanticdf

/** Internal visitor for walking a `SemanticOp` tree.
  *
  * Use cases for this adapter (R1 of the internal refactor audit):
  *  - "find the first occurrence of an op of type X" (set a field in
  *    `enter`, return early via a flag)
  *  - "collect every dimension/measure declared anywhere in this tree"
  *    (append to a `mutable.Map` in `enter`)
  *  - "log a debug line for every op we walk" (side-effecting in `enter`)
  *
  * The visitor walks the tree in **op-priority order**: outermost first
  * (joins), then their `source` chain (aggregate, filter, row filter,
  * orderBy, limit, hint, transforms). Joins are visited outermost,
  * with their left subtree walked first, then their right.
  *
  * Exhaustiveness. `enter` is a `def` on `SemanticOp` so an override
  * that uses a `match` benefits from the compiler's exhaustiveness
  * check. That property, combined with the visitor's automatic
  * recursion into all known wrappers, is what prevents the
  * "I added an op and 8 walkers MatchError'd at runtime" failure mode
  * tracked by `HintOpRegressionSpec`.
  *
  * Internal-only. `private[semanticdf]` so external code can't depend on
  * it; the public API of `SemanticTable` and `SemanticOp` is unchanged.
  */
private[semanticdf] abstract class SemanticOpVisitor {

  /** Called when the visitor enters an op node (before recursing into
    * children). Default does nothing; override to inspect or collect. */
  protected def enter(op: SemanticOp): Unit = ()

  /** Called after all children of `op` have been visited. Default does
    * nothing; override if you need post-order inspection. */
  protected def leave(op: SemanticOp): Unit = ()

  /** Set to `true` to stop the walk after the current op. The visitor
    * still calls `leave` on the current op before returning. */
  protected var stop: Boolean = false

  /** Walk `op` and its descendants, calling `enter` on the way in and
    * `leave` on the way out. The final op at line 58 of `SemanticOp`
    * is exhaustive: the compiler will fail if a new op type is added
    * without a corresponding case here. */
  final def visit(op: SemanticOp): Unit = {
    if (stop) return
    enter(op)
    if (stop) { leave(op); return }
    op match {
      case _: SemanticTableOp        => () // leaf: no children
      case j: SemanticJoinOp         => visit(j.left); visit(j.right)
      case a: SemanticAggregateOp    => visit(a.source)
      case f: SemanticFilterOp       => visit(f.source)
      case SemanticRowFilterOp(src, _, _, _, _) => visit(src)
      case o: SemanticOrderByOp      => visit(o.source)
      case l: SemanticLimitOp        => visit(l.source)
      case h: SemanticHintOp         => visit(h.source)
      case t: SemanticTransformsOp   => visit(t.source)
    }
    leave(op)
  }
}
