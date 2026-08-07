package io.semanticdf.core.engine

import io.semanticdf.core.model.Model

/** Engine-portable MCP engine-provider trait \u2014 Phase 2 contract.
  * Mirrors the design doc \u00a76.4 "MCPEngineProvider".
  *
  * ==Why a trait (not a concrete class)==
  *
  * Per scala-data-driven-refactor \u00a71: data in core, behavior in
  * adapters. The PROVIDER trait is the data shape (the contract
  * MCP needs); the IMPLEMENTATIONS (SparkEngineProvider,
  * TrinoEngineProvider) are the engine-specific behavior. The trait
  * lives in core; the implementations live in each engine adapter.
  *
  * ==Why `available: Boolean` (not just `identity: EngineIdentity`)==
  *
  * Per the design's `MCPEngineRegistry`: "the registry's `select`
  * filters availability". A provider can be registered but
  * unavailable (e.g. Spark Connect URL not configured; Trino cluster
  * not reachable). `available` is a runtime check, not a config
  * check.
  *
  * ==Why `query` returns `Either[EngineError, PortableQueryResult]`==
  *
  * Per the design \u00a76.4: every engine adapter's execute shape.
  * MCP consumers get a uniform `PortableQueryResult` shape (from PR
  * #400). `Either[EngineError, ...]` lets the registry surface typed
  * errors uniformly \u2014 not exceptions.
  *
  * ==Why `model: Model` (not `SemanticTable`)==
  *
  * The MCP is engine-portable. The `Model` is the engine-portable
  * shape (from core). The `SemanticTable` is the Spark-specific
  * shape (from the spark adapter). The provider receives a `Model`
  * and translates it to its engine's native shape internally. */
trait MCPEngineProvider {

  /** Wire-stable engine label. */
  def identity: EngineIdentity

  /** Runtime availability check. Per the design: "the registry's
    * `select` filters availability". `true` iff the provider is
    * configured AND can serve queries right now. */
  def available: Boolean

  /** Execute a query against this engine. Returns the
    * engine-portable `PortableQueryResult` (not the engine-native
    * shape).
    *
    * @param model    the portable model to query
    * @param request  the MCP query request shape (dimensions,
    *                 measures, where, having, orderBy, limit, etc.)
    * @param ctx      the engine context (timeout, cancellation,
    *                 audit policy, etc.)
    * @return          either a `PortableQueryResult` or a typed
    *                 `EngineError` (e.g. `EngineUnavailable`,
    *                 `ConnectionFailed`, `QueryTimedOut`) */
  def query(
      model:   Model,
      request: io.semanticdf.core.engine.MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, PortableQueryResult]

  /** Return a human-readable plan description (no execution).
    * Mirrors `Engine.explain`. Used by MCP's `explain` tool. */
  def explain(
      model:   Model,
      request: io.semanticdf.core.engine.MCPQueryRequest,
      ctx:     EngineContext,
  ): Either[EngineError, String]
}

/** Engine-portable MCP query-request shape. The shape is
  * engine-portable (no Spark, no Trino types). The provider
  * translates to its engine's native shape.
  *
  * ==Why no `where` / `having` / `orderBy` for v1==
  *
  * The `core.predicate.Predicate` and `predicate.Predicate`
  * (spark-adapter) are TWO different types per the round-3
  * DE review (Predicate type duplication). The MCP server
  * currently uses the spark-adapter Predicate for filter
  * translation. For PR 5, the engine-portable `MCPQueryRequest`
  * deliberately OMITS `where` / `having` — the MCP Query
  * handler in `semanticdf-mcp` keeps its own filter logic on
  * the legacy path. A future PR aligns the predicate types
  * (per the design's "Predicate consolidation" plan in
  * round-3 finding 6.2). */
final case class MCPQueryRequest(
    model:      String,
    dimensions: Seq[String] = Seq.empty,
    measures:   Seq[String] = Seq.empty,
    limit:      Option[Long] = None,
    timeGrain:  Option[String] = None,
    timeRange:  Option[(String, String)] = None,
) extends Product with Serializable

object MCPQueryRequest {

  /** Empty query \u2014 the canonical "zero filters" shape. */
  val empty: MCPQueryRequest = MCPQueryRequest(model = "")
}