---
name: scala-error-handling
description: Scala 2 error-handling and chained-computation standard for semanticdf's engine/compiler/model APIs. Use this whenever writing or reviewing Scala code that returns Either, Option, or Try; whenever deciding between throw vs Either vs Option; whenever writing a for-comprehension or .flatMap chain; whenever writing try/catch around JDBC, Spark, or JSON parsing; and whenever writing Spark UDFs, .map/.filter closures, or any executor-side (row-level) logic where error handling rules differ from driver-side code. Trigger on mentions of "Either", "error handling", "EngineError", "for-comprehension", "UDF error handling", or "executor serialization".
---

# Scala 2 error handling — semanticdf standard

**Status:** Standard. New code MUST follow. Existing code follows this where
it's natural to refactor (see "Known legacy exception" below).

This skill defines the error-handling and chained-computation style for
`semanticdf`. Every PR that touches public engine/compiler/model APIs, or
any Spark UDF/closure, should be checked against it.

**Scope note:** everything under "Driver-side" applies to plan/model
construction — code that runs once per query, on the driver, before
anything is submitted to Spark. It does NOT apply to code running inside a
UDF or a DataFrame/RDD transformation closure — that's executor-side, has
different cost tradeoffs (serialization, per-row allocation), and is
covered in its own section below. Don't copy driver-side `Either` patterns
into executor-side code.

---

## Three candidate styles (and why we picked A)

Three styles were considered before this standard was written:

- **A. Typed `Either[L, X]` + `flatMap` / for-comprehension + typed throws
  at boundaries** — the standard below. Compiler-enforced exhaustiveness,
  matches existing service-layer code (TrinoEngine, DuckDBEngine, MCP
  Query handler, Model.of, QueryBuilder). Picked.
- **B. Monadless (explicit `match` everywhere)** — forces every case to
  be considered at the use site. Considered; rejected because very
  verbose for chained ops and produces lots of temp vals.
- **C. Direct + `throw` everywhere** — familiar to Java devs; concise.
  Considered; rejected because throws escape (loss of type info) and
  conflate "silence is a symptom" (scala-chaos-testing §2).

This document describes style **A**. Apply it uniformly.

---

## Why

Errors are data. A typed sealed ADT (`EngineError`, `ModelValidationError`,
`CatalogError`) is the data; `Left(...)` is the carrier. Forcing every
public API to return `Either[L, X]` makes the failure mode visible at the
type level, and pattern-matching on the `Left` enforces exhaustive
handling — the compiler catches missed cases.

A `catch { case _: Exception => Left(...) }` that wraps everything into one
error case loses the specific failure mode: was it a network blip? a query
syntax error? an OOM? `EngineError.QueryRuntimeFailed` exists specifically
so callers can distinguish "connection failed" from "query runtime error."

## Driver-side: decision tree (check top to bottom, stop at first match)

1. **Is this a public API** (called across module boundaries, or part of
   Engine/Compiler/Model)?
   → Return type MUST be `Either[EngineError, X]` (or the module's error ADT).
   → No exceptions. No `Option`. No `Try`.

2. **Is this "may not exist" with no failure semantics** (a lookup, not a
   computation)?
   → Return `Option[X]`. Example: `Map.get`, `AggregateCall.input`.

3. **Is this a programmer error** (bad args, invariant violation, should
   never happen at runtime if callers are correct)?
   → `throw new IllegalArgumentException(...)` or
     `throw new IllegalStateException(...)`.
   → Do NOT wrap this in `Either`. Programmer errors are not data.
   → **Deprecated pattern to recognize in legacy code:**
     `throw new UnsupportedOperationException` used to mark "not yet
     implemented" at a boundary, with the caller catching + converting to
     `Either`. This is deprecated — see "Converter return types" example
     below for the replacement. Don't write new code this way; if you're
     touching a spot that does this, prefer converting it to the Either
     pattern instead of adding another instance.

4. **Is this an IO boundary** (JDBC, Spark DataFrame ops used from the
   driver, JSON parsing, file IO — anything where the underlying
   Java/Scala API itself throws)?
   → `try`/`catch` is allowed HERE ONLY.
   → Catch SPECIFIC exception types, not `case _: Exception`.
   → Convert immediately to the surrounding function's `Either[L, X]` at
     the bottom of the catch block. Don't let the exception escape the
     function.

5. **Is this an internal helper?**
   → Go to "Internal helper rule" below.

## Internal helper rule (Scala 2, no ambiguity)

Ask: how many call sites does this helper have, and is the result matched
immediately or threaded further?

- **ONE call site, caller does `match` on it right away**
  → Plain function, return `X` (not `Either[L, X]`).
  → Throw `IllegalArgumentException` for bad input.
  → DO NOT wrap in `Either` "for consistency." That's over-monading.

- **Feeds into a `for`-comprehension alongside 2+ other steps**
  → Return `Either[L, X]` where `L` is EXACTLY the same type as the
    `for`-comprehension's result type. Never a different/local ADT.

- **Converts between two ADTs** (e.g. legacy `Predicate` → `Expr`)
  → Return `Either[L, X]` directly. Never throw-then-catch across this
    boundary. `L` = the error type of whichever side is stricter. If a
    helper needs to surface "not supported yet," it returns
    `Either[EngineError.UnsupportedCapability, Expr]` directly — same
    shape as `TrinoQueryCompiler.compileRelOp` when it hits `RelOp.Join`
    (see `docs/design/v0.3.1-feature-parity-backlog.md` Gap 3).

### Converter return types — example

```scala
// Good (current pattern in TrinoQueryCompiler / DuckDBQueryCompiler):
def toExpr(p: Predicate, model: Model = null): Either[EngineError.UnsupportedCapability, Expr] = p match {
  case ... => Right(Expr.Equal(...))
  case Predicate.Compare.Contains(_, _) =>
    Left(EngineError.UnsupportedCapability(name = "Predicate.Contains", reason = "..."))
  case Expr.All(name) =>
    // Look up the measure; fail loud on programming error (unknown
    // measure). Throw IllegalArgumentException at the boundary; the
    // caller never catches this — the model's validator catches it
    // earlier.
    val _ = model.measures.find(_.name == name).getOrElse(
      throw new IllegalArgumentException(
        s"Expr.All('$name') references an unknown measure"))
    )
    Right(...)
}

// Bad (legacy pattern — do not copy):
def toExpr(p: Predicate): Expr = p match {
  case Predicate.Compare.Contains(_, _) =>
    throw new UnsupportedOperationException("Contains is not supported")
}
// + caller:
val expr = try Right(PredicateToExprConverter.toExpr(pred))
catch { case e: UnsupportedOperationException => Left(e.getMessage) }
// ^ loses the type info; returns Left(String) instead of
//   Left(EngineError.UnsupportedCapability)
```

**Replacement for the deprecated `throw new UnsupportedOperationException` at
a converter boundary**: return `Either[EngineError.UnsupportedCapability, X]`
directly. The exception-throw-and-catch round-trip loses the typed error that
the standard exists to preserve. Real-world example: PR #420 (v0.3.1 SQL
engine All lowerers) — the first draft used the throw pattern; the final
version returned `Left(...)` directly per this rule.

Default when unsure: plain function (bucket 1), not `Either`. Only promote
to `Either` when a real `for`-comprehension needs it.

## Chaining rule (Scala 2, when to use `for`)

Count the number of SEQUENTIAL steps where step N needs step N-1's success.

- **0 steps** (independent checks) → `if` guards with early `return Left(...)`
- **1 step** → call directly, `match` on result
- **2 steps** → `.flatMap` is OK, but `match` is usually clearer — pick either
- **3+ steps** → `for`-comprehension, MUST end in `yield` or a `match`

```scala
// 3+ steps -> for-comprehension
def compile(model: Model, ctx: EngineContext): Either[EngineError, ExecutionPlan[R]] = {
  if (ctx.cancelled) return Left(EngineError.CancellationFailed("cancelled by caller"))
  for {
    source   <- resolveSource(model.source)
    filtered <- applyFilters(source, model.filters)
    joined   <- applyJoins(filtered, model.joins)
  } yield applyAggregations(joined, model)
}

// 1 step -> direct call + match, NOT .flatMap
resolveSource(model.source) match {
  case Right(source) => useIt(source)
  case Left(err)      => handleError(err)
}
```

Use `match` when you need to react differently to specific error cases —
recover from known-safe failures, hard-fail on unexpected ones:

```scala
result match {
  case Right(value) => doSomething(value)
  case Left(EngineError.UnsupportedCapability(name, reason)) =>
    log.warn(s"engine skipped $name: $reason"); defaultValue()
  case Left(other) => throw new IllegalStateException(s"unexpected: $other")
}
```

Early `return Left(...)` at the top of a function for a linear guard chain
is fine and idiomatic — this matches what `Model.of`, `MCP Query.handle`,
and other entry-point functions already do. The rule: every early return
must use the same typed error ADT as the rest of the function.

## Hard bans (never do these, no exceptions)

- `Either[String, X]` — string is not a typed error, defeats the whole point.
- `Either[Throwable, X]` — same problem, just boxed differently.
- Catch-all exception wrapping — swallows the specific failure mode. Catch
  specific exception subtypes (`SQLException`, `IOException`, etc.)
  individually instead. See the worked example below.
- Throwing inside a function whose signature returns `Either[L, X]`.
- A `for`-comprehension with only 1-2 steps (unnecessary monad wrapping —
  use `match` or `.flatMap` instead per the chaining table).
- Mixing two different `L` error types in the same `for`-comprehension
  without an explicit `.left.map(lift)` at the seam.

### Worked example: catching failure modes at IO boundaries

```scala
// GOOD — distinguish failure modes (per scala-chaos-testing §2 "silence is a symptom")
try {
  compile(model, ctx)
} catch {
  case e: org.apache.spark.sql.AnalysisException =>
    // Spark rejected the query at plan time — a query-runtime issue
    Left(EngineError.QueryRuntimeFailed(
      reason = s"spark analysis failed: ${e.getClass.getSimpleName}: ${e.getMessage}"))
  case _: java.sql.SQLException =>
    // Couldn't reach the engine's backend
    Left(EngineError.ConnectionFailed(
      reason = s"spark JDBC failed: ${_}"))
}

// BAD — catch-all that loses information (per scala-chaos-testing §2)
try {
  compile(model, ctx)
} catch {
  case e: Exception => Left(EngineError.ConnectionFailed(
    reason = s"failed: ${e.getMessage}"))  // <- which failure mode is this?
}
```

**Note**: legacy code may still have the catch-all pattern. PR #418 + #420 added
`EngineError.QueryRuntimeFailed` to the `EngineError` ADT specifically so the
catch-all could be refined into typed cases. The legacy `SparkEngineProvider.runQuery`
still uses the catch-all (tracked for a separate refactor).

---

## Executor-side: Spark UDFs, closures, row-level logic

**This section overrides the driver-side rules above.** Code running
inside a UDF, `.map()`, `.filter()`, or any closure passed to a
DataFrame/RDD transformation executes once per row, per partition, on a
remote JVM. Two costs that don't exist on the driver apply here:

- **Serialization** — anything captured in the closure (including error
  ADTs) must be `Serializable` and closure-clean. A `Throwable`,
  `SparkSession`, or JDBC `Connection` captured inside an error case class
  will cause `NotSerializableException` at executor startup — not at
  compile time, so it's easy to miss in review.
- **Allocation pressure** — wrapping every row in `Either`/`Option` at
  millions/billions-of-rows scale is a measurable perf cost, unlike at
  plan-compile time where it happens once per query.

**Rules:**

1. **Do NOT propagate `Either`/typed ADTs per row.** Let Spark's own
   mechanisms handle bad records instead of hand-rolling a monad stack per
   record.

2. **Prefer Spark's built-in bad-record handling** where the input format
   supports it:
   - `spark.read.option("mode", "PERMISSIVE")` / `"badRecordsPath"` for
     parsing-stage failures.
   - For UDFs: catch narrowly inside the UDF and return `null` or a
     sentinel value. Never let an exception escape and kill the task/partition.

   ```scala
   // Executor-side (UDF) — plain try/catch, return null/sentinel, no Either
   val safeParse = udf((s: String) => {
     try {
       Some(parseValue(s))
     } catch {
       case _: NumberFormatException => None  // becomes null column-side
     }
   })
   ```

3. **If you need to know *why* rows failed**, don't return a rich error
   ADT per row. Use one of:
   - A lightweight `status`/`error_reason: String` column alongside the
     parsed value, or
   - An `Accumulator` to count failure categories (cheap, aggregates back
     to the driver), or
   - Splitting output into two DataFrames (`good`, `bad`) via `.filter` on
     a computed validity column.

4. **Never close over non-serializable state.** No `Throwable`,
   `SparkSession`, JDBC `Connection`, or driver-only object inside a case
   class or val that a UDF/closure captures. This is a closure-serialization
   trap, not a compile error — review closures for this explicitly.

5. **Keep exception types narrow, same as driver-side.** Catching broad
   `Exception` inside a hot-path UDF hides whether you're masking OOMs,
   real bugs, or expected bad data — "silence is a symptom" applies here
   too, it's just enforced by narrow `catch` + sentinel return instead of
   `Either`, since you can't return `Left(...)` up a `for`-comprehension
   from inside a `map`.

---

## Known legacy exception (do not copy this pattern in new code)

`SparkEngineProvider.runQuery` (pre-v0.3.1) still does:

```scala
} catch {
  case e: Exception => Left(EngineError.ConnectionFailed(s"spark.query failed: ${e.getMessage}"))
}
```

This is driver-side code (it triggers a Spark job, it doesn't run inside
one), and it's a known violation (catch-all, wrong error case for runtime
failures), tracked for cleanup. New code — see `runPortableQuery`
(PR #417) — uses `EngineError.QueryRuntimeFailed` for runtime errors and
reserves `EngineError.ConnectionFailed` for actual connection failures. If
you're editing near `runQuery`, don't imitate its catch-all; if you're
asked to fix it, split the catch by exception type.

## Cross-references

- **scala-data-driven-refacer** §1 (data is data, behavior lives elsewhere) — https://scala-data-driven-refacer
- **scala-chaos-testing** §2 (silence is a symptom)
- **scala-jvm-safety** §1 (null is a liar — applies to catch-all `Exception` too)
- **docs/design/v0.3.1-feature-parity-backlog.md** — gaps driving typed-Either adoption

### Related skills (apply in parallel, not in sequence)

This standard covers the error-handling layer. Five other skills cover
adjacent concerns that often surface in the same PR. Apply them when
relevant; cite the skill name in the PR's commit message when one of them
catches a finding.

- **scala-spark-batch-bugs** §1 (what you wrote isn't what runs) and §3
  (schema drift) — directly relevant to `PortableQueryCompiler.compile()`
  (window functions, partition pruning, lambda captures). Assert the
  actual numeric result, not just compile success.
- **scala-spark-streaming-bugs** (watermark / checkpoint / delivery /
  state) — relevant if/when streaming support lands. N/A for batch-
  only v0.3.1 work; list it in the PR's commit message if a finding
  applies.
- **scala-impact-analysis** §3 (binary compat) — directly relevant
  whenever a sealed-trait `case class` is added or a public signature
  changes (e.g. `Expr.All` in PR #419 added an exhaustive-match
  burden; `renderExpr` signature change in PR #420 rippled to ~20
  recursive call sites).
- **scala-perf-testing** — out of scope unless a benchmark shows
  regression. N/A for v0.3.1 v1.
- **scala-chaos-testing** §2 (silence is a symptom) — directly relevant
  when refactoring catch-alls. Worked example in the "Hard bans" section
  above.
