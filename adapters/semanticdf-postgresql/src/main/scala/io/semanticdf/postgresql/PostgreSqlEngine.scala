package io.semanticdf.postgresql

import io.semanticdf.core.engine.{
  Capability,
  Engine,
  EngineContext,
  EngineError,
  EngineIdentity,
  ExecutionPlan,
  PortableQueryResult,
  ResultSchema,
}
import io.semanticdf.core.model.Model
import io.semanticdf.core.rel.RelOp

/** v0.4.0: engine adapter for PostgreSQL via JDBC.
  *
  * Implements the engine-portable `Engine[R]` contract against a
  * [[PostgreSqlClient]] (typically a [[JdbcPostgreSqlClient]]).
  *
  * ==Scope (v1)==
  *
  * Per the user's plan: SELECT queries only. INSERT/UPDATE/DELETE
  * are out of engine-portable scope (defer to v0.4.0+).
  *
  * ==Why per-(database, schema, table) scoping==
  *
  * The engine carries a `database` name (used for source resolution)
  * and threads through to the underlying client. Multiple schemas
  * within the database are handled by the `SourceRef.ByName.namespace`
  * parameter (per the existing pattern in `TrinoQueryCompiler`).
  *
  * ==Error handling==
  *
  * Per `docs/design/error-handling-style.md`:
  *
  *   - All public methods return `Either[EngineError, X]` (typed ADT)
  *   - Map [[PostgreSqlError]] to [[EngineError]] SPECIFICALLY
  *     (no catch-all `ServerError`)
  *   - No `try/catch` at the boundary unless catching SPECIFIC
  *     JDK exception types
  *
  * ==Boundary contract==
  *
  * Zero Spark imports. */
final class PostgreSqlEngine(
    client:  PostgreSqlClient,
    database: String,
) extends Engine[Any] {

  /** Wire-stable engine label. Renaming is a breaking change to
    * MCP clients (`describe_model`, OKF generation, `audit_log`). */
  val identity: String = s"postgresql:$database"

  /** The set of typed capabilities this PostgreSQL engine supports.
    *
    * Per the design doc §4.5.3: closed `Set[Capability]`. We
    * advertise only the capabilities we actually deliver. */
  val capabilities: Set[Capability] = Set(
    Capability.NestedStructTypes,
  )

  /** Compile a portable [[Model]] to a PG SQL string.
    *
    * v1: minimal SQL subset (SELECT + FROM + WHERE + GROUP BY).
    * v0.4.0: window functions, CTEs, joins.
    *
    * Per error-handling-style.md: 1-step operation → direct call +
    * match. */
  override def compile(
      model: Model,
      ctx:    EngineContext,
  ): Either[EngineError, ExecutionPlan[Any]] = {
    try {
      val sql = modelToSql(model)
      Right(ExecutionPlan(
        engine               = EngineIdentity(
          name                 = identity,
          nativeVersion        = "1.0",
          engineAdapterVersion = "0.4.0",
        ),
        native               = sql,
        warnings             = Nil,
        requiredCapabilities = capabilities,
        normalizedSchema     = ResultSchema(Nil),
      ))
    } catch {
      case e: IllegalArgumentException =>
        // Per the standard: programmer errors at boundary throw
        // IllegalArgumentException. The adapter maps to a clear
        // failure mode (UnsupportedCapability) so the caller
        // knows the model has features we don't support.
        Left(EngineError.UnsupportedCapability(
          name   = "PostgreSqlEngine.compile",
          reason = s"model has features unsupported by v1 SQL subset: ${e.getMessage}",
        ))
    }
  }

  /** Compile a portable [[RelOp]] to a PG SQL string.
    *
    * For v1 we DON'T support direct RelOp compilation (the model
    * path goes through QueryBuilder + compile(model)). The error
    * is typed `EngineError.UnsupportedCapability` per the
    * standard ("use Left, not throw"). */
  override def compile(
      plan: RelOp,
      ctx:  EngineContext,
  ): Either[EngineError, ExecutionPlan[Any]] = {
    Left(EngineError.UnsupportedCapability(
      name   = "PostgreSqlEngine.compile(RelOp)",
      reason = "PostgreSQL engine runs SQL via JDBC; no RelOp-level translator. Use compile(Model) instead.",
    ))
  }

  override def execute(
      plan: ExecutionPlan[Any],
      ctx:  EngineContext,
  ): Either[EngineError, Any] = {
    val sql = plan.native.asInstanceOf[String]
    client.executeQuery(sql).left.map(withAction("execute"))
  }

  /** Override the default `executePortable` (which throws
    * `NotImplementedError` per the trait default) with a real
    * implementation. Per `docs/design/error-handling-style.md`,
    * `NotImplementedError` at this boundary is deprecated. */
  override def executePortable(
      plan: ExecutionPlan[Any],
      ctx:  EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    // Per the chaining rule: 3+ steps (execute + encode) → use
    // `for`-comprehension with `yield`.
    for {
      raw <- client.executeQuery(plan.native.asInstanceOf[String])
                       .left.map(postgreSqlToEngineError)
    } yield PostgreSqlResultEncoder.encode(raw)
  }

  override def explain(
      model: Model,
      ctx:    EngineContext,
  ): Either[EngineError, String] = {
    compile(model, ctx).map { plan =>
      s"PostgreSQL SQL:\n${plan.native}\nDatabase: $database"
    }
  }

  // -- helpers --

  /** Minimal v1 SQL builder: SELECT + FROM + WHERE + GROUP BY.
    *
    * Per error-handling-style.md "programmer error": throw
    * `IllegalArgumentException` at boundary for unhandled model
    * shapes (not `Left(...)` — the boundary is here). */
  private def modelToSql(model: Model): String = {
    val dims  = model.dimensions.map(d => s""""${d.name}"""")
    val meas  = model.measures.map { m =>
      val fnName = m.expr.fn.toString.toUpperCase
      // Per scala-jvm-safety: use the FieldRef's name field, not
      // toString (which includes the case class wrapper). For
      // Count (no input), use "*" as the standard SQL idiom.
      val input  = m.expr.input match {
        case Some(io.semanticdf.core.expr.Expr.FieldRef(name)) => name
        case Some(other) => other.toString  // fallback for other exprs
        case None => "*"
      }
      s"""$fnName($input) AS "${m.name}""""
    }
    val where = if (model.filters.isEmpty) ""
                else " WHERE " + model.filters.map(_.predicate.toString).mkString(" AND ")
    val groupBy = if (model.measures.isEmpty || model.dimensions.isEmpty) ""
                  else " GROUP BY " + dims.mkString(", ")
    // Per the design: SourceRef is a sealed ADT. ByName is the
    // case the model uses. Other shapes (ByPath, ByProvider) are
    // rejected at the SourceResolver layer, not here. Per
    // error-handling-style.md: programmer errors at boundary
    // throw IllegalArgumentException, NOT Either.
    val tableName = model.source match {
      case io.semanticdf.core.model.SourceRef.ByName(_, _, t) => t
      case other => throw new IllegalArgumentException(
        s"PostgreSQL engine only supports SourceRef.ByName; got ${other.getClass.getSimpleName}"
      )
    }
    val from = model.source match {
      case io.semanticdf.core.model.SourceRef.ByName(catalog, namespace, tableName) =>
        // Per scala-jvm-safety: bare-table form resolves against
        // the current search_path, which defaults to "$user, public"
        // (typical demo state). Don't hardcode database/schema so
        // the query works regardless of where the table was created.
        (catalog, namespace) match {
          case (Some(c), Some(s)) => s""""$c"."$s"."$tableName""""
          case (None,    Some(s)) => s""""$s"."$tableName""""
          case (_,       None)    => s""""$tableName""""
        }
      case other => throw new IllegalArgumentException(
        s"PostgreSQL engine only supports SourceRef.ByName; got ${other.getClass.getSimpleName}"
      )
    }
    val select = (dims ++ meas).mkString(", ")
    if (select.isEmpty) throw new IllegalArgumentException(
      s"model '${model.name}' has no dimensions or measures; cannot compile to a SELECT"
    )
    s"SELECT $select FROM $from$where$groupBy"
  }

  /** Map a [[PostgreSqlError]] to an [[EngineError]] case.
    *
    * Per error-handling-style.md "Hard bans": SPECIFIC failure modes,
    * not a generic `ServerError`. The `action` is inlined as the
    * error-message prefix (callers pass it via the call site so the
    * mapping function stays a single-arg `=>` per the standard's
    * .left.map contract). */
  private def postgreSqlToEngineError(err: PostgreSqlError): EngineError = {
    val typeName = err.getClass.getSimpleName
    val r        = reasonOf(err)
    // The "action" prefix is added at the call site (via .left.map
    // composition or by re-invoking this method with the action
    // baked in). Per error-handling-style.md, error messages should
    // include the action context for debuggability — but the
    // mapping function is pure (single-arg).
    err match {
      case PostgreSqlError.ConnectionFailed(_)     => EngineError.ConnectionFailed(reason = s"$typeName: $r")
      case PostgreSqlError.AuthenticationFailed(_) => EngineError.ConnectionFailed(reason = s"$typeName: $r")
      case PostgreSqlError.TableNotFound(_)        => EngineError.SourceSchemaChanged(source = s"$typeName: $r")
      case PostgreSqlError.ColumnNotFound(_)       => EngineError.QueryRuntimeFailed(reason = s"$typeName: $r")
      case PostgreSqlError.SyntaxError(_)          => EngineError.QueryRuntimeFailed(reason = s"$typeName: $r")
      case PostgreSqlError.UniqueViolation(_)      => EngineError.QueryRuntimeFailed(reason = s"$typeName: $r")
      case PostgreSqlError.CheckViolation(_)       => EngineError.QueryRuntimeFailed(reason = s"$typeName: $r")
      case PostgreSqlError.CasConflict(_)          => EngineError.QueryRuntimeFailed(reason = s"$typeName: $r")
      case PostgreSqlError.NetworkError(_)         => EngineError.ConnectionFailed(reason = s"$typeName: $r")
      case PostgreSqlError.Interrupted(_)          => EngineError.ConnectionFailed(reason = s"$typeName: $r")
      case PostgreSqlError.PoolExhausted(_)        => EngineError.ConnectionFailed(reason = s"$typeName: $r")
      case PostgreSqlError.MalformedResponse(_)    => EngineError.QueryRuntimeFailed(reason = s"$typeName: $r")
    }
  }

  /** Wrap the mapping with an action prefix (for error messages).
    * Per error-handling-style.md, every error case surfaces the
    * action context so callers can debug. */
  private def withAction(action: String)(err: PostgreSqlError): EngineError = {
    val engineErr = postgreSqlToEngineError(err)
    // Prepend the action to the reason for debuggability.
    engineErr match {
      case EngineError.ConnectionFailed(reason)     => EngineError.ConnectionFailed(reason = s"$action: $reason")
      case EngineError.QueryRuntimeFailed(reason)   => EngineError.QueryRuntimeFailed(reason = s"$action: $reason")
      case EngineError.SourceSchemaChanged(source)  => EngineError.SourceSchemaChanged(source = s"$action: $source")
      case other => other
    }
  }

  private def reasonOf(e: PostgreSqlError): String = e match {
    case PostgreSqlError.ConnectionFailed(r)     => r
    case PostgreSqlError.AuthenticationFailed(r) => r
    case PostgreSqlError.TableNotFound(r)        => r
    case PostgreSqlError.ColumnNotFound(r)       => r
    case PostgreSqlError.SyntaxError(r)          => r
    case PostgreSqlError.UniqueViolation(r)      => r
    case PostgreSqlError.CheckViolation(r)       => r
    case PostgreSqlError.CasConflict(r)          => r
    case PostgreSqlError.NetworkError(r)         => r
    case PostgreSqlError.Interrupted(r)          => r
    case PostgreSqlError.PoolExhausted(r)        => r
    case PostgreSqlError.MalformedResponse(r)    => r
  }
}

object PostgreSqlEngine {

  /** Smart constructor. */
  def apply(
      client:   PostgreSqlClient,
      database: String,
  ): PostgreSqlEngine = new PostgreSqlEngine(client, database)
}