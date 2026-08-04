package io.semanticdf.core.rel

import io.semanticdf.core.engine.ResolvedSource
import io.semanticdf.core.expr.Expr
import io.semanticdf.core.schema.Field

/** Engine-portable relational-plan IR — Phase 2 contract. Mirrors
  * the design doc §4.5.2 "RelOp" (8 cases total: Scan, Filter,
  * Project, Aggregate, Join, Sort, Limit, plus a placeholder for
  * set ops deferred to v0.4.0).
  *
  * `RelOp` is the engine-portable shape of a relational query plan.
  * It flows through:
  *   - the model's compile step (`Engine.compile(model: RelOp, ...)`)
  *   - the engine adapter's expression-compile step (each case
  *     becomes a native operation — Spark `Dataset` ops, Trino SQL
  *     clauses, etc.)
  *   - the MCP wire format (for `explain` tool output)
  *
  * ==Why a sealed ADT (not a String tree)==
  *
  * The design's "Capabilities describe what an engine supports"
  * principle applies: `RelOp` is the engine-portable plan shape.
  * A free-form `plan: String` field would let engines invent new
  * plan shapes that the validator and compiler couldn't classify.
  * A closed ADT forces every component to handle the closed set
  * of plan nodes.
  *
  * ==Why core (engine-portable)==
  *
  * The relational plan IR is universal across query engines.
  * Every SQL engine has scan / filter / project / aggregate /
  * join / sort / limit. The engine-specific compile (Spark's
  * `LogicalPlan`, Trino's `LogicalPlanner`, etc.) lives in the
  * engine adapter.
  *
  * ==Why 8 cases (not fewer, not more)==
  *
  * The set covers the plan nodes needed by the portable model:
  *   - **Scan (1)** — read a source's resolved scan
  *   - **Filter (1)** — apply a predicate to a child
  *   - **Project (1)** — compute expressions into named columns
  *   - **Aggregate (1)** — group by + aggregate calls
  *   - **Join (1)** — combine two children with a join kind + condition
  *   - **Sort (1)** — order a child by sort keys
  *   - **Limit (1)** — take a slice of a child
  *
  * Set operations (`Union`, `Intersect`, `Except`), window
  * functions, and streaming sinks are DEFERRED to v0.4.0 per the
  * design. They are not a regression — they were never in the
  * portable model — and they can be expressed via combination of
  * the existing nodes if needed.
  *
  * ==Why `predicate: Expr` (not `Predicate`) on `Filter`==
  *
  * The relational IR is the RUNTIME execution IR. At runtime,
  * filters are expressions (e.g. `price > 100`). The higher-level
  * `Predicate` filter language (in `core.predicate.Predicate`)
  * gets compiled into `Expr` for the IR. The IR carries `Expr`
  * because that's what engines actually execute.
  *
  * ==Why `projection: List[Expr]` on `Scan`==
  *
  * A `Scan` is a read from a source. The `projection` lists the
  * columns to read (column pruning). The `schema` lists the
  * expected schema (for source-drift detection; the actual schema
  * after resolution is `ResolvedScan.fields`). The `source` is
  * the resolved-source result (carrying the original `SourceRef`
  * for provenance).
  *
  * ==Data-driven mantra compliance==
  *
  * - Pure data: sealed trait + 8 case classes
  * - Equality auto-derived (case classes)
  * - Hash code stable (auto-derived)
  * - `Product with Serializable`
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. Verifiable by:
  * `grep -r 'org.apache.spark' semanticdf-core/src/main/scala/io/semanticdf/core/rel/RelOp.scala`
  */
sealed trait RelOp extends Product with Serializable

object RelOp {

  /** Read from a resolved source. The terminal node of any plan.
    *
    * @param source     the resolved source (carries the original
    *                   `SourceRef` for provenance + the resolver's
    *                   result: `Scan` / `Incompatible` /
    *                   `AuthFailed` / `NotFound`)
    * @param schema     the expected schema (after source resolution
    *                   — the engine adapter validates the actual
    *                   source's schema matches this; a mismatch
    *                   yields `EngineError.SourceSchemaChanged`)
    * @param projection the columns to read (column pruning — the
    *                   engine adapter reads only these from the
    *                   source, not the full set)
    */
  final case class Scan(
      source:     ResolvedSource,
      schema:     List[Field],
      projection: List[Expr],
  ) extends RelOp

  /** Apply a predicate to a child. Maps to Spark's `Filter`,
    * Trino's `WHERE` clause.
    *
    * @param input     the child node
    * @param predicate the predicate expression (returns a boolean)
    */
  final case class Filter(
      input:     RelOp,
      predicate: Expr,
  ) extends RelOp

  /** Compute expressions into named columns. Maps to Spark's
    * `Project`, Trino's `SELECT` clause.
    *
    * @param input       the child node
    * @param expressions the projected expressions and their
    *                    aliases (the `String` is the alias)
    */
  final case class Project(
      input:       RelOp,
      expressions: List[(Expr, String)],
  ) extends RelOp

  /** Group by expressions and apply aggregate calls. Maps to
    * Spark's `Aggregate`, Trino's `GROUP BY` clause.
    *
    * @param input      the child node
    * @param groupBy    the group-by expressions (the columns to
    *                   partition by)
    * @param aggregates the aggregate calls (Sum / Count / Avg /
    *                   etc.)
    */
  final case class Aggregate(
      input:      RelOp,
      groupBy:    List[Expr],
      aggregates: List[AggregateCall],
  ) extends RelOp

  /** Combine two children with a join kind and an optional
    * condition. Maps to Spark's `Join`, Trino's `JOIN` clause.
    *
    * @param left      the left child
    * @param right     the right child
    * @param kind      the join kind (Inner / Left / Right / Full
    *                  / Cross)
    * @param condition the join condition (for `Cross`, this is
    *                  unused — the join is unconditional)
    */
  final case class Join(
      left:      RelOp,
      right:     RelOp,
      kind:      JoinKind,
      condition: Expr,
  ) extends RelOp

  /** Order a child by sort keys. Maps to Spark's `Sort`, Trino's
    * `ORDER BY` clause.
    *
    * @param input the child node
    * @param keys  the sort keys (each is an `Expr` + direction +
    *              null ordering)
    */
  final case class Sort(
      input: RelOp,
      keys:  List[SortKey],
  ) extends RelOp

  /** Take a slice of a child. Maps to Spark's `Limit`, Trino's
    * `LIMIT ... OFFSET ...` clause.
    *
    * @param input  the child node
    * @param count  the maximum number of rows to return
    * @param offset the number of rows to skip before returning
    */
  final case class Limit(
      input:  RelOp,
      count:  Long,
      offset: Long = 0L,
  ) extends RelOp
}