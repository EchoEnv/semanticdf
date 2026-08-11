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
import io.semanticdf.core.expr.{Expr, LiteralValue}
import io.semanticdf.core.model.{FilterSpec, Model}
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
      val (sql, params) = modelToSql(model)
      Right(ExecutionPlan(
        engine               = EngineIdentity(
          name                 = identity,
          nativeVersion        = "1.0",
          engineAdapterVersion = "0.4.0",
        ),
        // Per C1 fix (2026-08-11): thread WHERE params through the
        // plan as a `(sql, params)` tuple. Bind-parameterized SQL
        // closes the SQL-injection vector that `_.predicate.toString`
        // concatenation created. See `renderPredicate` for the
        // parameter-mapping implementation.
        native               = (sql, params),
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
    val (sql, params) = plan.native.asInstanceOf[(String, Seq[Any])]
    client.executeQuery(sql, params).left.map(withAction("execute"))
  }

  /** Override the default `executePortable` (which throws
    * `NotImplementedError` per the trait default) with a real
    * implementation. Per `docs/design/error-handling-style.md`,
    * `NotImplementedError` at this boundary is deprecated. */
  override def executePortable(
      plan: ExecutionPlan[Any],
      ctx:  EngineContext,
  ): Either[EngineError, PortableQueryResult] = {
    val native = plan.native.asInstanceOf[(String, Seq[Any])]
    // Per the chaining rule: 3+ steps (execute + encode) → use
    // `for`-comprehension with `yield`.
    for {
      raw <- client.executeQuery(native._1, native._2)
                       .left.map(postgreSqlToEngineError)
    } yield PostgreSqlResultEncoder.encode(raw)
  }

  override def explain(
      model: Model,
      ctx:    EngineContext,
  ): Either[EngineError, String] = {
    compile(model, ctx).map { plan =>
      val (sql, params) = plan.native.asInstanceOf[(String, Seq[Any])]
      val paramsLine = if (params.isEmpty) "" else s"\nBind params: $params"
      s"PostgreSQL SQL:\n$sql$paramsLine\nDatabase: $database"
    }
  }

  // -- helpers --

  /** Minimal v1 SQL builder: SELECT + FROM + WHERE + GROUP BY.
    *
    * Returns `(sql, params)` — the WHERE clause uses bind-parameter
    * placeholders (`?`) and the params are passed to
    * `JdbcPostgreSqlClient.executeQuery` so user-supplied filter
    * values never reach the SQL parser as text.
    *
    * Per error-handling-style.md "programmer error": throw
    * `IllegalArgumentException` at boundary for unhandled model
    * shapes (not `Left(...)` — the boundary is here). */
  private def modelToSql(model: Model): (String, Seq[Any]) = {
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
    // Per C1 fix (2026-08-11): parameterized WHERE. The previous
    // implementation concatenated `_.predicate.toString` into the
    // SQL string, which was a SQL-injection vector. See
    // `renderPredicate` for the type-safe rendering.
    val (whereSql, whereParams) = renderWhere(model.filters)
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
    (s"SELECT $select FROM $from$whereSql$groupBy", whereParams)
  }

  /** Render a `Seq[FilterSpec]` to a parameterized WHERE clause
    * fragment + the bind params in order.
    *
    * Per C1 fix (2026-08-11): the previous code built the WHERE
    * via `_.predicate.toString` concatenation, which let any
    * filter value's `toString` reach the SQL parser verbatim
    * (e.g. `Expr.Equal(FieldRef("c"), Literal("'; DROP TABLE..."))`
    * would emit garbage like `Equal(FieldRef(c),Literal(...))`).
    * This helper replaces that with a typed `Expr → SQL` walk
    * that emits `?` placeholders for every literal value.
    *
    * Per `scala-error-handling §1` (errors are data) + `scala-jvm-safety`
    * boundary: unknown Expr cases throw
    * `IllegalArgumentException` at this boundary — same contract
    * as `modelToSql`. */
  private def renderWhere(filters: Seq[FilterSpec]): (String, Seq[Any]) = {
    if (filters.isEmpty) ("", Seq.empty[Any])
    else {
      val rendered = filters.map(f => renderExpr(f.predicate))
      val sql      = rendered.map(_._1).mkString(" AND ")
      val params   = rendered.flatMap(_._2)
      (s" WHERE $sql", params)
    }
  }

  /** Render a single portable `Expr` to `(sql, params)`. Recursive
    * over compound nodes. Field refs are double-quoted (PG
    * identifier quoting); literal values become `?` placeholders
    * bound to the params list in pre-order traversal order.
    *
    * Per C1 fix (2026-08-11): every `Literal` contributes its
    * value to the params list and a `?` to the SQL. No literal
    * value's `toString` reaches the SQL parser. */
  private def renderExpr(e: Expr): (String, Seq[Any]) = e match {
    // -- Leaves --
    case Expr.Literal(value, _) =>
      // Per scala-jvm-safety §1: trust nothing from user input;
      // every Literal value goes through JDBC's PreparedStatement
      // binding, not SQL string interpolation.
      (s"?", Seq(literalToJava(value)))
    case Expr.FieldRef(name)    => (s""""$name"""", Seq.empty[Any])
    case Expr.MeasureRef(name)  => (s""""$name"""", Seq.empty[Any])
    case Expr.All(measureName)  =>
      // Per scala-spark-batch-bugs: percent-of-total resolves to a
      // window sum across all rows of the current group.
      (s"""SUM("$measureName") OVER ()""", Seq.empty[Any])

    // -- Arithmetic --
    case Expr.Add(l, r) =>
      val (ls, lp) = renderExpr(l); val (rs, rp) = renderExpr(r)
      (s"($ls + $rs)", lp ++ rp)
    case Expr.Subtract(l, r) =>
      val (ls, lp) = renderExpr(l); val (rs, rp) = renderExpr(r)
      (s"($ls - $rs)", lp ++ rp)
    case Expr.Multiply(l, r) =>
      val (ls, lp) = renderExpr(l); val (rs, rp) = renderExpr(r)
      (s"($ls * $rs)", lp ++ rp)
    case Expr.Divide(l, r) =>
      val (ls, lp) = renderExpr(l); val (rs, rp) = renderExpr(r)
      (s"($ls / $rs)", lp ++ rp)
    case Expr.Modulo(l, r) =>
      val (ls, lp) = renderExpr(l); val (rs, rp) = renderExpr(r)
      (s"($ls % $rs)", lp ++ rp)

    // -- Comparison --
    case Expr.Equal(l, r) =>
      renderBinaryCmp(l, r, "=")
    case Expr.NotEqual(l, r) =>
      renderBinaryCmp(l, r, "<>")
    case Expr.LessThan(l, r) =>
      renderBinaryCmp(l, r, "<")
    case Expr.LessOrEqual(l, r) =>
      renderBinaryCmp(l, r, "<=")
    case Expr.GreaterThan(l, r) =>
      renderBinaryCmp(l, r, ">")
    case Expr.GreaterOrEqual(l, r) =>
      renderBinaryCmp(l, r, ">=")

    // -- Boolean --
    case Expr.And(l, r) =>
      val (ls, lp) = renderExpr(l); val (rs, rp) = renderExpr(r)
      (s"($ls AND $rs)", lp ++ rp)
    case Expr.Or(l, r) =>
      val (ls, lp) = renderExpr(l); val (rs, rp) = renderExpr(r)
      (s"($ls OR $rs)", lp ++ rp)
    case Expr.Not(inner) =>
      val (is, ip) = renderExpr(inner)
      (s"(NOT $is)", ip)

    // -- Null checks --
    case Expr.IsNull(inner) =>
      val (is, ip) = renderExpr(inner)
      (s"($is IS NULL)", ip)
    case Expr.IsNotNull(inner) =>
      val (is, ip) = renderExpr(inner)
      (s"($is IS NOT NULL)", ip)

    // -- Cast --
    case Expr.Cast(inner, targetType) =>
      val (is, ip) = renderExpr(inner)
      // Per scala-spark-batch-bugs §3: type fidelity differs across
      // engines. For v1, only INT/BIGINT/VARCHAR casts are emitted
      // — others throw at the boundary (per karpathy §2: minimum
      // code that solves the problem).
      val pgType = renderPgType(targetType)
      (s"CAST($is AS $pgType)", ip)

    // -- Function call --
    case Expr.FunctionCall(name, args) =>
      val rendered = args.map(renderExpr)
      val sql      = rendered.map(_._1).mkString(", ")
      val params   = rendered.flatMap(_._2)
      (s"""$name($sql)""", params)
  }

  /** Helper for the 6 comparison cases — same shape: render both
    * sides, emit `<left> <op> <right>`, concat params in order.
    *
    * Per `scala-error-handling §3` (chaining rule): 3-step operation
    * (render l + render r + concat) → helper. */
  private def renderBinaryCmp(
      l:   Expr,
      r:   Expr,
      op:  String,
  ): (String, Seq[Any]) = {
    val (ls, lp) = renderExpr(l)
    val (rs, rp) = renderExpr(r)
    (s"($ls $op $rs)", lp ++ rp)
  }

  /** Render a portable `SealedDataType` to a PG `CAST AS` target.
    * Limited subset — other types throw at the boundary. */
  private def renderPgType(t: io.semanticdf.core.schema.SealedDataType): String = t match {
    case io.semanticdf.core.schema.SealedDataType.Int        => "INT"
    case io.semanticdf.core.schema.SealedDataType.BigInt     => "BIGINT"
    case io.semanticdf.core.schema.SealedDataType.Varchar    => "VARCHAR"
    case io.semanticdf.core.schema.SealedDataType.Boolean    => "BOOLEAN"
    case other => throw new IllegalArgumentException(
      s"PostgreSQL CAST AS $other not yet supported in v1"
    )
  }

  /** Extract the underlying Java value from a portable `LiteralValue`
    * so JDBC's `PreparedStatement.setObject(idx, v)` can bind it.
    *
    * Per `scala-jvm-safety §1`: this is the ONLY place the literal
    * value crosses from the portable boundary to JDBC — type-faithful
    * extraction, no `toString` interpolation.
    *
    * Complex types (Map, Struct) throw — they need a portable
    * representation the PG driver can bind, which is out of scope
    * for v1. */
  private def literalToJava(v: LiteralValue): Any = v match {
    case LiteralValue.IntValue(x)       => x
    case LiteralValue.ByteValue(x)      => x
    case LiteralValue.ShortValue(x)     => x
    case LiteralValue.LongValue(x)      => x
    case LiteralValue.FloatValue(x)     => x
    case LiteralValue.DoubleValue(x)    => x
    case LiteralValue.DecimalValue(x)   => x
    case LiteralValue.StringValue(x)    => x
    case LiteralValue.BoolValue(x)      => x
    case LiteralValue.BinaryValue(x)    => x.toArray
    case LiteralValue.TimestampValue(x) => java.sql.Timestamp.from(x)
    case LiteralValue.DateValue(x)      => java.sql.Date.valueOf(x)
    case LiteralValue.ArrayValue(xs)    => xs.map(literalToJava).toArray
    case LiteralValue.MapValue(_)       =>
      throw new IllegalArgumentException("MapValue literals not supported in v1 PG SQL")
    case LiteralValue.StructValue(_)    =>
      throw new IllegalArgumentException("StructValue literals not supported in v1 PG SQL")
    case LiteralValue.NullValue         => null
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