package io.semanticdf.core.model

import io.semanticdf.core.expr.Expr
import io.semanticdf.core.expr.Expr.MeasureRef

/** Engine-portable calc-DAG helper — Phase 2 contract.
  *
  * A private helper used by [[ModelValidator]] to check that the
  * model's calculated-measure DAG is acyclic and within the
  * engine's depth cap.
  *
  * ==Why a separate object (vs. inline in `ModelValidator`)==
  *
  * `CalcGraph.checkAcyclicAndDepth` is a SELF-CONTAINED piece of
  * logic: it walks a graph and reports a cycle / depth violation.
  * Extracting it into a separate object:
  *   - Makes the validator cleaner (`ModelValidator` does shape
  *     checks + delegates depth checks to `CalcGraph`)
  *   - Lets us test `CalcGraph` independently (no need to construct
  *     a full `Model` for every cycle test)
  *   - Keeps the cycle-detection algorithm visible (vs. buried in
  *     a 200-line validator)
  *
  * ==Why core (engine-portable)==
  *
  * The DAG shape (a `Map[String, Set[String]]` of measure -> refs)
  * is universal across engines. The DEPTH CAP is engine-specific
  * (Spark supports deep calc DAGs; SQL engines cap at e.g. 5; the
  * cap is passed as a parameter, not baked into the algorithm).
  *
  * ==Data-driven mantra compliance==
  *
  * `CalcGraph` is an `object` (singleton), not a class. Its methods
  * are pure functions of their inputs. No state, no engine coupling.
  *
  * ==Algorithm==
  *
  * BFS-based cycle detection:
  *   - Build adjacency: `Map[calcName -> Set[calcName]]` of every
  *     calc measure to the calc measures it references.
  *   - For each calc measure, BFS forward counting depth. If we
  *     re-visit a node (already in the current path), that's a
  *     cycle.
  *   - Track max depth across all calc measures.
  *
  * @see ModelValidator.validate check (4) — the caller passes the
  *      engine-specific `maxDepthBound` (e.g. `Int.MaxValue` for SQL
  *      engines, a tighter cap for the spark adapter).
  */
private[model] object CalcGraph {

  /** Check the calc-DAG for cycles and depth violations.
    *
    * @param calcNames          the set of declared calc-measure names
    * @param calculatedMeasures the list of declared calc measures
    * @param maxDepthBound      the engine's depth cap (e.g. 5 for SQL,
    *                           `Int.MaxValue` for the spark adapter)
    * @return `Right(depth)` if acyclic and within the cap,
    *         `Left(depth)` if a cycle is detected (depth is the
    *         length of the cycle path) */
  def checkAcyclicAndDepth(
      calcNames:          Set[String],
      calculatedMeasures: List[CalculatedMeasure],
      maxDepthBound:      Int,
  ): Either[Int, Int] = {
    // Build adjacency: calc -> referenced calc names
    val adjacency: Map[String, Set[String]] = calculatedMeasures.map { cm =>
      val refs = collectCalcRefs(cm.expr).filter(calcNames.contains)
      cm.name -> refs
    }.toMap

    // Track max depth across all calc measures
    var maxDepth = 0

    // Detect cycles via DFS + visited set
    def dfs(node: String, depth: Int, visiting: Set[String]): Option[Int] = {
      if (visiting.contains(node)) Some(depth)  // cycle detected
      else {
        if (depth > maxDepth) maxDepth = depth
        if (maxDepth > maxDepthBound) Some(maxDepth)
        else {
          val neighbors = adjacency.getOrElse(node, Set.empty)
          neighbors.foldLeft(Option.empty[Int]) { (acc, next) =>
            acc.orElse(dfs(next, depth + 1, visiting + node))
          }
        }
      }
    }

    // Run DFS from every calc measure
    val cycle = calcNames.foldLeft(Option.empty[Int]) { (acc, start) =>
      acc.orElse(dfs(start, depth = 0, visiting = Set.empty))
    }

    cycle match {
      case Some(depth) => Left(depth)
      case None        => Right(maxDepth)
    }
  }

  /** Collect all `MeasureRef` names from an `Expr` tree. Reuses
    * the [[Expr]] shape (no engine coupling). Pure function:
    * walks the tree, returns the set of measure refs. */
  private def collectCalcRefs(e: Expr): Set[String] = {
    def go(x: Expr, acc: Set[String]): Set[String] = x match {
      case Expr.Literal(_, _)               => acc
      case Expr.FieldRef(_)                 => acc
      case Expr.MeasureRef(name)            => acc + name
      case Expr.Add(left, right)            => go(left, go(right, acc))
      case Expr.Subtract(left, right)       => go(left, go(right, acc))
      case Expr.Multiply(left, right)       => go(left, go(right, acc))
      case Expr.Divide(left, right)         => go(left, go(right, acc))
      case Expr.Modulo(left, right)         => go(left, go(right, acc))
      case Expr.Equal(left, right)          => go(left, go(right, acc))
      case Expr.NotEqual(left, right)       => go(left, go(right, acc))
      case Expr.LessThan(left, right)       => go(left, go(right, acc))
      case Expr.LessOrEqual(left, right)    => go(left, go(right, acc))
      case Expr.GreaterThan(left, right)    => go(left, go(right, acc))
      case Expr.GreaterOrEqual(left, right) => go(left, go(right, acc))
      case Expr.And(left, right)            => go(left, go(right, acc))
      case Expr.Or(left, right)             => go(left, go(right, acc))
      case Expr.Not(expr)                   => go(expr, acc)
      case Expr.IsNull(expr)                => go(expr, acc)
      case Expr.IsNotNull(expr)             => go(expr, acc)
      case Expr.Cast(expr, _)               => go(expr, acc)
      case Expr.FunctionCall(_, args)       => args.foldLeft(acc)((a, arg) => go(arg, a))
      case Expr.All(name)                   => acc + name
    }
    go(e, Set.empty)
  }
}