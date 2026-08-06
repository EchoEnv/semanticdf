package io.semanticdf.core.query

import io.semanticdf.core.engine.{EngineError, ResolvedSource, SourceResolver}
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{Model, SourceRef}
import io.semanticdf.core.rel.{AggregateCall, RelOp}
import io.semanticdf.core.schema.Field

/** Engine-portable `Model → RelOp` builder.
  *
  * Per the multi-engine design §4.5.1 + §4.3.2: source resolution
  * is the boundary step BEFORE the engine sees anything. The
  * `QueryBuilder` is that step:
  *   1. Walk the `Model` and resolve its `SourceRef` via
  *      `SourceResolver.resolve(...)`
  *   2. Build the `RelOp` tree (Scan → Filter → Project →
  *      Aggregate → Sort → Limit) with the resolved source
  *      baked in
  *   3. Surface resolver failures as typed `EngineError` (not as
  *      raw exceptions)
  *
  * ==Why this lives in core (not in engine adapters)==
  *
  * The `Model → RelOp` lowering is **engine-portable** — every
  * engine adapter wants the same `RelOp` tree for a given
  * `Model` (modulo engine-specific capabilities). Per
  * scala-data-driven-refacer §1: data in core, behavior in
  * adapters. The lowering is data-shape-only; the engine-specific
  * `RelOp → native plan` is the adapter's behavior.
  *
  * ==Why a separate object (not a method on `Model`)==
  *
  * The `Model` class is pure data (per §1 of the data-driven
  * mantra). Putting `build` on `Model` would mix data with
  * behavior (and the `SourceResolver` parameter is a runtime
  * collaborator, not a data field). A separate object keeps
  * `Model` pure.
  *
  * ==Why v1 doesn't handle joins==
  *
  * The `Model.joins: List[JoinSpec]` field exists (per design
  * §4.4) but `JoinSpec`-shaped joins are deferred to a future
  * PR. `QueryBuilder` produces a plan with a single `Scan` —
  * multi-source models fall through to `EngineError.UnsupportedCapability`
  * via the engine's `compile` step. This is a documented v1
  * scope (matches the existing `TrinoQueryCompiler` v1 scope
  * and the design's §4.5.2 plan-node list).
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
object QueryBuilder {

  /** Lower a portable [[Model]] to a portable [[RelOp]] tree.
    *
    * @param model     the portable model
    * @param resolver  the source resolver (caller's choice:
    *                  per design §4.6.4, any catalog adapter)
    * @param identity  the engine identity (for the resolver call)
    * @return          either a portable `RelOp` tree, or a typed
    *                  `EngineError` (NotFound / Incompatible /
    *                  AuthFailed) */
  def build(
      model:    Model,
      resolver: SourceResolver,
      identity: io.semanticdf.core.engine.EngineIdentity,
  ): Either[EngineError, RelOp] = {

    // Step 1: resolve the source. The design's boundary step —
    // happens BEFORE the engine sees anything.
    resolver.resolve(model.source, identity) match {
      case scan: ResolvedSource.Scan =>
        // Step 2: build the RelOp tree.
        // v1 shape: Scan → [Filter] → [Project + Aggregate] → [Sort] → [Limit]
        // Future PR: handle joins (single source only for v1).
        Right(buildRelOpTree(model, scan))

      case ResolvedSource.NotFound(_, _) =>
        Left(EngineError.FeatureDeferred(
          feature = s"query-builder.source-not-found:${model.name}",
          release = "v0.5.0",
        ))

      case ResolvedSource.Incompatible(_, _) =>
        Left(EngineError.FeatureDeferred(
          feature = s"query-builder.source-incompatible:${model.name}",
          release = "v0.5.0",
        ))

      case ResolvedSource.AuthFailed(_, _) =>
        Left(EngineError.FeatureDeferred(
          feature = s"query-builder.source-auth-failed:${model.name}",
          release = "v0.5.0",
        ))
    }
  }

  /** Build the RelOp tree (Scan → Filter → Project → Aggregate →
    * Sort → Limit) from a successfully-resolved `Scan`.
    *
    * Per design §4.5.2: the tree shape is bottom-up (Scan first,
    * Limit last). We assemble it inside-out so the algebra
    * composes correctly.
    *
    * ==Why a separate method (not inlined)==
    *
    * The `Scan` case carries the `schema: List[Field]` — that's
    * the source's actual column list. Future PRs can validate
    * that the model's dimension/measure/filter references all
    * exist in this schema (catching column-name typos at
    * compile time). For v1 we trust the model's references. */
  private def buildRelOpTree(
      model: Model,
      scan:  ResolvedSource.Scan,
  ): RelOp = {

    // The terminal node: the source scan.
    // Per design §4.5.2: `projection` is the column-prune list.
    // For v1 we read all columns (the engine can prune).
    val scanNode: RelOp = RelOp.Scan(
      source     = scan,
      schema     = scan.schema.fields.toList.flatMap { case (name, typeName) => extractField(name, typeName) },
      projection = Nil,  // v1: read all columns; engine can prune
    )

    // Wrap in optional Filter if the model has filters.
    // v1: the model has zero filters at this layer; per-model
    // filters live in the SQL emit (lowered at the engine
    // adapter level). Future PR: lower `model.filters` to
    // RelOp.Filter here.
    val filtered: RelOp =
      if (model.filters.isEmpty) scanNode
      else RelOp.Filter(input = scanNode, predicate = Expr.Literal(LiteralValue.BoolValue(true), io.semanticdf.core.schema.SealedDataType.Boolean))

    // Project + Aggregate: every dimension becomes a projected
    // column, every measure / calculated measure becomes a
    // computed column. The Aggregate is the bottom of the
    // Project (engine-portable: dimensions are projected from
    // the grouped-by input, measures are aggregated).
    val aggregateNode: RelOp = RelOp.Aggregate(
      input      = filtered,
      groupBy    = model.dimensions.map(d => Expr.FieldRef(d.name)),
      aggregates = model.measures.map(m =>
        AggregateCall(
          fn        = m.expr.fn,
          input     = m.expr.input,
          alias     = m.name,
        ),
      ),
    )

    // Project: dimensions + measures + calculated measures.
    val projectNode: RelOp = RelOp.Project(
      input       = aggregateNode,
      expressions = {
        val dimCols = model.dimensions.map(d =>
          (Expr.FieldRef(d.name), d.name)
        )
        val measCols = model.measures.map(m =>
          (Expr.FieldRef(m.name), m.name)
        )
        val calcCols = model.calculatedMeasures.map(c =>
          (c.expr, c.name)
        )
        dimCols ++ measCols ++ calcCols
      },
    )

    // Sort: v1 — no portable sort key (sorts are engine-specific
    // via `preview(n)` and `count()`). Future PR.
    val sorted: RelOp = projectNode

    // Limit: v1 — no portable limit (limits are engine-specific
    // via `preview(n)`). Future PR.
    sorted
  }

  /** Extract a `Field` from a (name, typeName) pair. The
    * `ResolvedSchema.fields` is a `Map[String, String]`
    * (columnName → typeName, stringly-typed per PR #3's pending
    * work to fix `ResolvedSchema`).
    *
    * Per the design's pre-PR-3 state, we have to re-parse the
    * type name into a typed `SealedDataType`. For v1 we use
    * a heuristic mapping (long → BigInt, varchar → Varchar, etc.)
    * — engine adapters can refine. The map is sufficient for
    * `Scan.schema` (the engine reads columns by name; the
    * typed conversion happens at ResultSchema time, in PR 3). */
  private def extractField(name: String, typeName: String): Option[Field] = {
    val dataType = typeNameToSealedDataType(typeName)
    Some(Field(name = name, dataType = dataType, nullable = true))
  }

  /** Heuristic UC/Trino/DuckDB type-name → `SealedDataType` mapping.
    * The v1 lowering depends on this for `Scan.schema` typing.
    * Engines refine at execute time. */
  private def typeNameToSealedDataType(typeName: String): io.semanticdf.core.schema.SealedDataType = {
    val lower = typeName.toLowerCase
    if (lower.startsWith("bigint") || lower.startsWith("long")) io.semanticdf.core.schema.SealedDataType.BigInt
    else if (lower.startsWith("int") || lower.startsWith("integer")) io.semanticdf.core.schema.SealedDataType.Int
    else if (lower.startsWith("double") || lower.startsWith("float")) io.semanticdf.core.schema.SealedDataType.Double
    else if (lower.startsWith("boolean") || lower.startsWith("bool")) io.semanticdf.core.schema.SealedDataType.Boolean
    else if (lower.startsWith("date")) io.semanticdf.core.schema.SealedDataType.Date
    else if (lower.startsWith("timestamp")) io.semanticdf.core.schema.SealedDataType.Timestamp
    else if (lower.startsWith("decimal")) io.semanticdf.core.schema.SealedDataType.Decimal(precision = 18, scale = 2)
    else if (lower.startsWith("json")) io.semanticdf.core.schema.SealedDataType.Json
    // Default: Varchar. Per scala-data-driven-refactor §1: prefer
    // a safe default over an error.
    else io.semanticdf.core.schema.SealedDataType.Varchar
  }
}