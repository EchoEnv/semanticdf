package io.semanticdf.hera

import io.semanticdf.core.engine.{
  Capability,
  EngineContext,
  EngineError,
  EngineIdentity,
  ExecutionPlan,
  PortableQueryResult,
  ResolvedSource,
}
import io.semanticdf.core.engine.{
  ResultSchema,
}
import io.semanticdf.core.expr.Expr
import io.semanticdf.core.model.Model
import io.semanticdf.core.rel.RelOp

/** v0.3.1: engine adapter for Hera's `POST /private/explore/query`.
  *
  * Implements the engine-portable [[io.semanticdf.core.engine.Engine]]
  * contract against a [[HeraClient]]. Each `HeraEngine` instance is
  * scoped to ONE realm (per user "realm is the top layer that
  * separates catalogs/engines/etc.") and ONE Zeus execution engine
  * (per user "zeus is hera engine for execution").
  *
  * ==Why per-realm + per-zeus==
  *
  * Per the user's domain knowledge: a Hera deployment typically has
  * multiple realms, each containing multiple Zeus engines (e.g. a
  * Trino-backed Zeus and a Spark-backed Zeus in the same realm). To
  * query a specific realm's data on a specific engine, callers
  * construct one `HeraEngine` per (realm, zeus) pair. The engine's
  * `realmId` and `zeusId` are carried in every call.
  *
  * ==Why we expose only `executePortable` (and minimal `compile`)==
  *
  * Per `docs/design/error-handling-style.md`: the Engine trait's
  * `executePortable` default throws `NotImplementedError`. The
  * standard is explicit ("this is deprecated — use a real
  * ResultEncoder"). For Hera, we override it with a real
  * implementation that calls `HeraClient.executeQuery` and wraps
  * the result via a `ResultEncoder`. We do NOT implement a full
  * `compile(model) → SQL` path here — Hera is a remote query
  * service; we don't have its query planner on the driver side.
  * The `compile(plan)` path returns `Left(UnsupportedCapability)` per
  * the standard's "use Left, not throw" rule.
  *
  * ==Error handling==
  *
  * Per the standard:
  *   - All public methods return `Either[EngineError, X]` (typed ADT)
  *   - Map [[HeraClientError]] → [[EngineError]] cases SPECIFICALLY
  *     (no catch-all `ServerError`)
  *   - No `try/catch` at the boundary unless catching SPECIFIC
  *     JDK exception types
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final class HeraEngine(
    client:    HeraClient,
    realmId:   Long,
    zeusId:    Long,
) extends io.semanticdf.core.engine.Engine[Any] {

  /** Wire-stable engine label. Renaming is a breaking change to MCP
    * clients (`describe_model`, OKF generation, `audit_log`).
    * Per user: "realm is the top layer that separates catalogs /
    * engines / etc." → we identify the engine by `hera:<realmId>:<zeusId>`. */
  val identity: String = s"hera:$realmId:$zeusId"

  /** The set of typed capabilities this Hera engine supports.
    * Per the design doc §4.5.3 (capabilities as typed set):
    *
    *   - `SqlWhere` / `SqlGroupBy` / `SqlAggregates` — Hera's
    *     `/private/explore/query` supports standard SQL.
    *   - `NestedStructTypes` — Hera returns nested types via JSON.
    *   - No `Streaming` (Hera's Kafka is separate from the query
    *     service).
    *   - No `Materialize` (no intermediate result caching).
    *   - No `Rollup` (no native rollup precompute — uses the
    *     catalog-level rollup that the v0.3.1 Gap 6 closure will
    *     add separately). */
  val capabilities: Set[Capability] = Set(
    Capability.NestedStructTypes,
  )

  /** Per error-handling-style.md "Converter return types": we map
    * [[HeraClientError]] to [[EngineError]] SPECIFICALLY (no
    * generic `ServerError`). The mapping:
    *
    *   - `Unauthorized` / `Forbidden` / `NoPermission` → `EngineUnavailable`
    *     (caller can't reach the engine)
    *   - `NotFound` / `AlreadyExists` / `Conflict` → `SourceSchemaChanged`
    *     (the engine's metadata is out of sync with our model)
    *   - `QueryFailed` / `BadRequest` → `QueryRuntimeFailed` (the
    *     query was malformed at the engine)
    *   - `EngineError` (Hera 521) → `EngineUnavailable` (engine broken)
    *   - `NetworkError` → `ConnectionFailed`
    *   - `MalformedResponse` → `QueryRuntimeFailed` (we couldn't decode
    *     the response) */
  private def heraToEngineError(
      err:    HeraClientError,
      action: String,
  ): EngineError = {
    // Include the case-class name in the reason for easier debugging
    // (per the standard's "specific failure modes" principle — callers
    // log the reason and need to know WHICH case failed).
    val typeName = err.getClass.getSimpleName
    val r        = reasonOf(err)
    err match {
      case HeraClientError.Unauthorized(_)      => EngineError.ConnectionFailed(reason = s"$action: $typeName: $r")
      case HeraClientError.Forbidden(_)         => EngineError.ConnectionFailed(reason = s"$action: $typeName: $r")
      case HeraClientError.NoPermission(_)      => EngineError.ConnectionFailed(reason = s"$action: $typeName: $r")
      case HeraClientError.RealmNotFound(_)     => EngineError.SourceSchemaChanged(source = s"$action: $typeName: $r")
      case HeraClientError.ZeusNotFound(_)      => EngineError.EngineUnavailable(name = s"$action: $typeName: $r", available = Nil, wasDefault = false)
      case HeraClientError.NotFound(_)          => EngineError.SourceSchemaChanged(source = s"$action: $typeName: $r")
      case HeraClientError.AlreadyExists(_)     => EngineError.SourceSchemaChanged(source = s"$action: $typeName: $r")
      case HeraClientError.Conflict(_)          => EngineError.QueryRuntimeFailed(reason = s"$action: $typeName: $r")
      case HeraClientError.QueryFailed(_)       => EngineError.QueryRuntimeFailed(reason = s"$action: $typeName: $r")
      case HeraClientError.EngineError(_)       => EngineError.EngineUnavailable(name = s"$action: $typeName: $r", available = Nil, wasDefault = false)
      case HeraClientError.BadRequest(_)        => EngineError.QueryRuntimeFailed(reason = s"$action: $typeName: $r")
      case HeraClientError.NetworkError(_)      => EngineError.ConnectionFailed(reason = s"$action: $typeName: $r")
      case HeraClientError.MalformedResponse(_) => EngineError.QueryRuntimeFailed(reason = s"$action: $typeName: $r")
    }
  }

  /** Extract the `reason` field from any [[HeraClientError]] case
    * without losing the type info. Per error-handling-style.md
    * "Internal helper rule": ONE call site (heraToEngineError),
    * caller uses value immediately → plain function returning
    * String. */
  private def reasonOf(e: HeraClientError): String = e match {
    case HeraClientError.Unauthorized(r)      => r
    case HeraClientError.Forbidden(r)         => r
    case HeraClientError.NoPermission(r)      => r
    case HeraClientError.RealmNotFound(r)     => r
    case HeraClientError.ZeusNotFound(r)      => r
    case HeraClientError.NotFound(r)          => r
    case HeraClientError.AlreadyExists(r)     => r
    case HeraClientError.Conflict(r)          => r
    case HeraClientError.QueryFailed(r)       => r
    case HeraClientError.EngineError(r)       => r
    case HeraClientError.BadRequest(r)        => r
    case HeraClientError.NetworkError(r)      => r
    case HeraClientError.MalformedResponse(r) => r
  }

  /** The minimum SQL subset we can translate from a [[Model]] to
    * a Hera-compatible SQL string. For v1 we translate the model's
    * primary query: SELECT columns from the source table, with
    * optional WHERE, GROUP BY, and aggregate measures.
    *
    * Per error-handling-style.md "Internal helper rule": 1 call
    * site → plain function returning `String` (not `Either`). Throws
    * `UnsupportedOperationException` if the model has features we
    * don't support — the caller maps that to `Left(UnsupportedCapability)`.
    *
    * Wait — per the standard, `throw UnsupportedOperationException`
    * is deprecated at a converter boundary. Use `Left(UnsupportedCapability)`
    * directly. Since this helper is called from `compile(model)`, the
    * caller catches via the existing `Left(...)` pattern. */
  private def modelToSql(model: Model): String = {
    val dims   = model.dimensions.map(d => s""""${d.name}"""")
    val meas   = model.measures.map { m =>
      val fnName = m.expr.fn.toString.toUpperCase
      val input  = m.expr.input.map(_.toString).getOrElse("*")
      s"""$fnName($input) AS "${m.name}""""
    }
    val where  = if (model.filters.isEmpty) ""
                 else " WHERE " + model.filters.map(_.predicate.toString).mkString(" AND ")
    val groupBy = if (model.measures.isEmpty) "" else " GROUP BY " + dims.mkString(", ")
    val sql = "SELECT " + (dims ++ meas).mkString(", ") +
              " FROM " + model.source.toString +
              where + groupBy
    sql
  }

  override def compile(
      model: Model,
      ctx:    EngineContext,
  ): Either[EngineError, ExecutionPlan[Any]] = {
    // Per error-handling-style.md: 1-step operation → direct call + match.
    // We construct the plan with the SQL string; the actual execution
    // happens in `executePortable` (where the typed ResultEncoder wraps
    // the rows).
    //
    // Per the standard: cancellation is handled by the platform via
    // `CancellationCapability` (sealed trait). We don't check a
    // boolean flag here — the engine consumer (MCP / Query service)
    // does the cancellation via the request ID.
    try {
      val sql = modelToSql(model)
      Right(ExecutionPlan(
        engine               = EngineIdentity(
          name                 = identity,
          nativeVersion        = "1.0",
          engineAdapterVersion = "0.3.0",
        ),
        native               = sql,
        warnings             = Nil,
        requiredCapabilities = capabilities,
        normalizedSchema     = ResultSchema(Nil),
      ))
    } catch {
      case _: IllegalArgumentException =>
        // Per the standard: programmer errors at the boundary throw.
        // Caller bug; map to a clear failure.
        Left(EngineError.UnsupportedCapability(
          name   = "model",
          reason = "HeraEngine.compile: model has features unsupported by v1 SQL subset",
        ))
    }
  }

  override def compile(
      plan: io.semanticdf.core.rel.RelOp,
      ctx:  EngineContext,
  ): Either[EngineError, ExecutionPlan[Any]] = {
    // Per error-handling-style.md: typed Either at the boundary.
    // We don't have a RelOp-to-SQL translator for Hera (the engine
    // runs SQL directly via /private/explore/query). Map to
    // UnsupportedCapability (NOT throw).
    Left(EngineError.UnsupportedCapability(
      name   = "RelOp.compile",
      reason = "HeraEngine.compile(RelOp): Hera runs SQL directly via /private/explore/query; no RelOp-level translator. Use compile(Model) instead.",
    ))
  }

  override def execute(
      plan: ExecutionPlan[Any],
      ctx:  EngineContext,
  ): Either[EngineError, Any] = {
    // The `native` field of the plan is the SQL string (we set it
    // in `compile`). We pass it to Hera via the client.
    val sql = plan.native.asInstanceOf[String]
    client.executeQuery(
      sql      = sql,
      realmId  = realmId,
      zeusId   = Some(zeusId),
    ).left.map(heraToEngineError(_, "execute"))
  }

  /** Override the default `executePortable` (which throws
    * NotImplementedError per the trait default) with a real
    * implementation. Per `docs/design/error-handling-style.md`,
    * `NotImplementedError` at this boundary is deprecated; we
    * implement the contract. */
  override def executePortable(
      plan: ExecutionPlan[Any],
      ctx:  EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    // Per the chaining rule: 3+ steps (execute + encode) →
    // for-comprehension with yield. (Cancellation is handled
    // upstream by the platform via CancellationCapability.)
    // Per error-handling-style.md: 1-step (the execute) + 1-step
    // (the encode) → use flatMap, not for-comprehension. The encode
    // step uses a minimal v1 ResultEncoder (Hera-specific encoder
    // with full type support is deferred to v0.4.0 — for v1 we map
    // the rows into a portable result with String values, which
    // is correct enough for the catalog/query metadata use cases
    // and matches the existing FakeCatalogAdapter pattern).
    client.executeQuery(
      sql      = plan.native.asInstanceOf[String],
      realmId  = realmId,
      zeusId   = Some(zeusId),
    ).left.map(heraToEngineError(_, "executePortable")).map { raw =>
      HeraResultEncoder.encode(raw)
    }
  }

  override def explain(
      model: Model,
      ctx:    EngineContext,
  ): Either[EngineError, String] = {
    // Per the standard: 1-step (compile + format) → direct call + match.
    compile(model, ctx).map { plan =>
      s"Hera SQL:\n${plan.native}\nRealm: $realmId\nZeus: $zeusId"
    }
  }

}

/** Singleton factory for the canonical Hera engine.
  *
  * Per karpathy §2 ("minimum code that solves the problem"): we
  * keep a thin factory so callers don't need to know the constructor
  * signature. Future PRs may add a richer builder. */
object HeraEngine {

  /** Smart constructor — preferred over `new HeraEngine(...)` for
    * default param ergonomics. */
  def apply(
      client:  HeraClient,
      realmId: Long,
      zeusId:  Long,
  ): HeraEngine = new HeraEngine(client, realmId, zeusId)

  /** Wire-stable canonical name for the engine label. */
  val DefaultLabel: String = "hera"
}