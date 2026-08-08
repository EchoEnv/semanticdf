# Error-handling style

**Status:** Standard. New code MUST follow. Existing code follows this where it's natural to refactor (see "Catch-all cleanup" at the bottom).

This document defines the error-handling and chained-computation style for `semanticdf`. Every PR that touches public engine/compiler/model APIs should be checked against it.

## The rule of thumb

| Construct | When | Example |
|---|---|---|
| **`Either[L, X]`** | Public API; multi-step internal chain | `Engine.compile(model, ctx): Either[EngineError, ExecutionPlan[R]]` |
| **`Option[X]`** | "May not exist" only | `Map.get`, `AggregateCall.input: Option[Expr]` |
| **`throw new IllegalArgumentException`** | Programming errors (invalid args) | `Predicate.Compare.apply("unknown_op", ...)` |
| **`throw new UnsupportedOperationException`** | "Not yet implemented" at a clear boundary, with the immediate caller catching + converting to `Either` | (deprecated — see "Converter return types" below) |
| **`try` / `catch`** | ONLY at IO boundaries (JDBC close, Spark `DataFrame` ops, JSON parsing where the underlying API throws) | `spark.table(...)` translation; JDBC `close()` |

## Why

Per **scala-data-driven-refacer §1** (data is data, behavior lives elsewhere), errors are data. A typed sealed ADT (`EngineError`, `ModelValidationError`, `CatalogError`) is the data; the `Left(...)` is the carrier. Forcing every public API to return `Either[L, X]` makes the failure mode visible at the type level; pattern-matching on the `Left` enforces exhaustive handling (the compiler catches missed cases).

Per **scala-chaos-testing §2** ("silence is a symptom"), a `catch { case _: Exception => Left(...) }` that wraps everything into one error case loses the specific failure mode (was it a network blip? a query syntax error? an OOM?). The pre-v0.3.0 audit added `EngineError.QueryRuntimeFailed` exactly so callers can distinguish "connection failed" from "query runtime error" — but legacy code still uses the catch-all.

## Internal helpers

Internal helpers in the same call chain use `Either[L, X]` with the SAME `L` as the public API (or the error ADT of the immediate caller). **Not** `Either[String, ...]`, **not** `Either[Throwable, ...]`, **not** a throw-then-catch round-trip.

If a helper genuinely needs to surface "this legacy predicate isn't supported yet", it returns `Either[EngineError.UnsupportedCapability, Expr]` directly — same shape as `TrinoQueryCompiler.compileRelOp` when it encounters `RelOp.Join` (see `docs/design/v0.3.1-feature-parity-backlog.md` Gap 3).

## Chaining

For multi-step linear chains, prefer `for`-comprehensions when you have 3+ steps:

```scala
val result = for {
  source <- resolveSource(model.source)
  filtered <- applyFilters(source, model.filters)
  joined <- applyJoins(filtered, model.joins)
  grouped <- Right(applyAggregations(joined, model))
} yield grouped
```

For 1-2 step chains, use `.flatMap` directly:

```scala
val result = resolveSource(model.source).flatMap { source =>
  applyFilters(source, model.filters)
}
```

Use `match` when you need to react to specific error cases:

```scala
result match {
  case Right(value) => doSomething(value)
  case Left(EngineError.UnsupportedCapability(name, reason)) =>
    log.warn(s"engine skipped $name: $reason"); defaultValue()
  case Left(other) => throw new IllegalStateException(s"unexpected: $other")
}
```

## Early returns

At the **top** of a function with a linear guard chain, early `return Left(...)` is fine and idiomatic (matches what `Model.of`, `MCP Query.handle`, and other entry-point functions do). The rule is: every early return must use the **same typed error ADT**, and the chain must end with a `match` that covers every case (or an exhaustive `for`-comprehension).

```scala
def compile(model: Model, ctx: EngineContext): Either[EngineError, ExecutionPlan[R]] = {
  if (ctx.cancelled) return Left(EngineError.CancellationFailed("cancelled by caller"))
  for {
    source <- resolveSource(model.source)
    ...
  } yield ...
}
```

## Converter return types

A converter between two ADTs (e.g. legacy `Predicate` → portable `Expr`) returns `Either[L, X]` where `L` is the **same error type** as the caller's boundary. Throwing and catching across the boundary is anti-pattern — it loses the type info that `Either` exists to preserve.

```scala
// Good (Standard A):
def toExpr(p: Predicate): Either[EngineError.UnsupportedCapability, Expr] = p match {
  case ... => Right(Expr.Equal(...))
  case Predicate.Compare.Contains(_, _) =>
    Left(EngineError.UnsupportedCapability(name = "Predicate.Contains", reason = "..."))
}

// Bad (legacy patterns observed in the codebase):
def toExpr(p: Predicate): Expr = p match {
  case Predicate.Compare.Contains(_, _) =>
    throw new UnsupportedOperationException("Contains is not supported")
}
// + caller:
val expr = try Right(PredicateToExprConverter.toExpr(pred))
catch { case e: UnsupportedOperationException => Left(e.getMessage) }
// ^ loses the type info; returns Left(String) instead of Left(EngineError.UnsupportedCapability)
```

## Catch-all cleanup (in progress)

The legacy `SparkEngineProvider.runQuery` (pre-v0.3.1) catches all `Exception` and wraps as `EngineError.ConnectionFailed`:

```scala
} catch {
  case e: Exception => Left(EngineError.ConnectionFailed(
    reason = s"spark.query failed: ${e.getMessage}",
  ))
}
```

This conflates "couldn't reach the engine" with "query runtime error" — per scala-chaos-testing §2, this is a "silence is a symptom" finding.

**Refactor plan (start small)**: new code (`runPortableQuery` introduced in PR #417) uses `EngineError.QueryRuntimeFailed` for runtime errors and `EngineError.ConnectionFailed` only for actual connection errors. Legacy `runQuery` is updated in a follow-up PR — tracked in the v0.3.1 backlog.

## Cross-references

- **scala-data-driven-refacer** §1 (data is data, behavior lives elsewhere) — https://scala-data-driven-refacer
- **scala-chaos-testing** §2 (silence is a symptom)
- **scala-jvm-safety** §1 (null is a liar — but applies to errors too: catch-all `Exception` is a "lie" that says nothing about the cause)
- **docs/design/v0.3.1-feature-parity-backlog.md** — the gaps that drive the typed-Either adoption
