# Phase E — Type Safety via Typeclasses

**Trigger:** First real consumer surfaces runtime error pain (string typos, wrong-type operations) — or proactively if the team has seen enough synthetic-data testing to know the pain is real.

**Constraint:** No API-breaking changes to existing `Dimension`, `Measure`, `Predicate` public APIs. New type-safe variants coexist with old ones. Migrate existing users incrementally.

---

## Why typeclasses

SemanticDF's current API is stringly-typed at the edges:

```scala
// All of these are runtime errors:
t("total_passengerrs")          // typo — fails at execute
t("avg_passengers") / t("count") // wrong type — crashes at execute
st.where("carrier" > 600)      // String > Int — runtime type error
st.aggregate("total_passengerrs") // typo in measure name — runtime
```

Scala's type system can catch these at compile time. The typeclass pattern adds **zero runtime overhead** and requires **no breaking API changes** if done as additive variants.

---

## What's in scope (three targeted additions)

### E1 — ResultDecoder: typed query results

> **Status (2026-07):** `ResultDecoder[T]` typeclass and `collectAs[T]`
> terminal shipped in PR `#52`. `ResultDecoder.derive[T]` Scala 2 macro
> for case classes with primitive fields shipped in PR `#64`.
> **Shipped (v0.1.7):** `SemanticTable.queryAs[T]: Dataset[T]` — a Spark
> `Encoder`-flavored return type for typed inputs. (Originally named
> `query[T]` in the plan, renamed to `queryAs[T]` for consistency with
> `collectAs[T]`.) Bundles `query(...)`'s parameters and adds a typed
> conversion via the implicit `ResultDecoder[T]` + Spark `Encoder[T]`.
> 5 new tests in `QueryAsSpec`; 353 library tests pass total.

**The highest-value, lowest-risk addition.** After building and executing the op tree, decode the `DataFrame` into a strongly-typed case class.

```scala
// --- User code ---

// Define the output shape
case class CarrierRevenue(carrier: String, totalPassengers: Long, avgPassengers: Double)

// Derive the decoder automatically
given ResultDecoder[CarrierRevenue] = ResultDecoder.derive

// Execute and get a typed Dataset — COMPILE-TIME type safety
val result: Dataset[CarrierRevenue] =
  model.groupBy("carrier")
    .aggregate("total_passengers", "avg_passengers")
    .query[CarrierRevenue]("carrier", "total_passengers", "avg_passengers")

// Wrong case class field? COMPILE ERROR:
// case class CarrierRevenue(carrier: String, totalPassengerrs: Long, ...)
// → error: value totalPassengerrs is not a member of CarrierRevenue

// Wrong field type? COMPILE ERROR:
// case class CarrierRevenue(carrier: Int, ...)  (Int instead of String)
// → error: type mismatch: found Int, required String
```

**Implementation:** `ResultDecoder[T]` typeclass + auto-derivation via `Dataset` reflection (one Spark `Encoder` call). Users either derive automatically or provide a manual instance.

**Scope:** `SemanticTable.query[T]` returns `Dataset[T]`. `toDataFrame` and `execute` stay unchanged (backward compatible).

**Effort:** ~1 day. Bounded, isolated, high value.

---

### E2 — Typed Dimension[T] / TypedMeasure[T]

**Add phantom type to dimension and measure declarations.** The lambda still resolves columns at runtime (Spark schema), but the phantom type constrains operations.

```scala
// --- Old API (still works, unchanged) ---
Dimension("carrier", t => t("carrier"))
Measure("total_passengers", t => sum(t("passengers")))

// --- New typed variants (coexist) ---
Dimension[String]("carrier", t => t("carrier"))
Measure[Long]("total_passengers", t => sum(t("passengers")))
Measure[Double]("avg_passengers",  t => t[Long]("total_passengers") / t[Long]("flight_count"))
```

**What this enables:**

```scala
// Type-safe calc formula (E3 builds on this)
Measure[Double]("avg_passengers", t =>
  divide(t[Long]("total_passengers"), t[Long]("flight_count"))
)
// avg_passengers: Long / Long → Double ✓
// avg_passengers: String / Long → COMPILE ERROR ✓
```

**Implementation:**
- `Dimension[T]` / `Measure[T]` are new case classes extending/alongside the existing untyped ones
- `TypedMeasure` carries the phantom `T` and a DSL builder that only allows type-compatible operations
- The **existing** untyped APIs stay — no migration needed

**What's explicitly NOT done in E2:**
- No `NumericExpr` typeclass that provides `+`, `-`, `*`, `/` on all numeric types
- No constraint that prevents `Measure[Long].expr` from being used where `Double` is expected
- These are deferred (they need a full `NumericExpr` design and add significant complexity)

**Effort:** ~2 days.

---

### E3 — Typed arithmetic within calc measures

**Building on E2.** Once measures have phantom types, provide a small typed arithmetic DSL within calc lambdas.

```scala
// Untyped (still works):
Measure("avg_passengers", t => t("total_passengers") / t("flight_count"))

// Typed (new):
Measure[Double]("avg_passengers", t =>
  divide[Long, Long, Double](t("total_passengers"), t("flight_count"))
)
```

The `divide[N: Numeric, D: Numeric, R]` function checks types at compile time. This is **opt-in** — untyped measures continue to work exactly as before.

**What this catches at compile time:**
- `t[String]("total") / t[String]("count")` — COMPILE ERROR (String has no Numeric)
- `t[Long]("a") + t[String]("b")` — COMPILE ERROR (no implicit Numeric[String])
- `divide(t("x"), t("y"))` where `x` and `y` are different numeric types — depends on implicits

**Implementation:**
- `NumericExpr` / `Divide` / `Multiply` etc. as zero-runtime wrappers over Spark `Column`
- The lambda returns `Column` in both typed and untyped paths — Spark's execution is unchanged

**What's explicitly deferred from E3:**
- Implicit conversions from `Column` to `NumericExpr` (ambiguous with Spark's own implicits — resolves to Spark's ops, not typed ones)
- Type inference for return types (Scala's local type inference is insufficient for deep chains)
- Auto-derivation of `Numeric` for complex Spark types

**Effort:** ~2 days. Highest risk of the three (Spark's implicit `Column` → `Column` operations are already well-worn; wrapping them with stricter types can cause surprising conflicts).

---

## Implementation status (what's actually shipped)

The original three-target plan above is **partial**: E2/E3-style phantom typing shipped as
the `SemanticField[T]` typeclass instead of `Dimension[T]` / `Measure[T]`, because field
references (not field declarations) were the actual pain point — typos happen at the call
site (`st.groupBy("carierr")`), not at the declaration.

### Done — typed field references (PR #7)

- `SemanticField[T]` phantom-typed typeclass with `SemanticDimension[T]` / `SemanticMeasure[T]` subtypes.
- `groupByDimensions[D1..D4]` / `aggregateMeasures[M1..M4]` typed overloads; `…All(refs)` for arity 5+.
- Typed `Predicate.Eq/Ne/Gt/Ge/Lt/Le/in/notIn/isNull/isNotNull[F](ref, v)` factories.
- `FieldRef[T]` carrier is a value-class wrapper — zero runtime overhead on the hot path.
- 9 regression tests in `SemanticFieldSpec.scala` verifying typed output === string output.

### Done — sealed `Predicate.Compare` ADT (PR #8)

- `sealed trait Compare` with sealed `Eq`/`Ne`/`Lt`/`Le`/`Gt`/`Ge` case classes; operator
  choice encoded in the type, not a runtime string.
- Backward-compatible `Compare.apply(op, field, value)` factory preserved.
- Each case has its own `compile()` / `describe()` — no string dispatch per compile.
- 8 regression tests in `PredicateSpec.scala` verifying identical output to the legacy form.

### Done — typed `withMeasures` + `SortKey.asc/desc` overloads (PR #24, v0.1.1)

- `SortKey.asc(field: SemanticField[_])` / `SortKey.desc(field: SemanticField[_])` accept the
  typeclass instance directly via subtyping, so the typed overload is picked over
  `asc(name: String)` even from cross-package consumer code (Scala 2.13 phase-1 overload
  resolution matches by subtyping without needing an implicit conversion).
- `withMeasures[F](measure: SemanticMeasure[F], expr, ...)` — same subtype-based approach;
  the measure name is read from the witness. Both overloads funnel through a private
  `withMeasures0` helper.
- 4 new tests in `SemanticFieldSpec` (282 total at this point).

### Done — YAML load-time validation pass (PRs #25–#27, v0.1.1)

- `ExpressionValidator` (PR #25) parses every `dimensions:`, `transforms:`, and `measures:`
  `expr` at load time via Spark's `CatalystSqlParser` and checks that every column
  reference resolves against the visible columns at that point. A typo fails fast at
  model-load time with a clear error, not later at first query time as a cryptic Spark
  `UNRESOLVED_COLUMN.WITH_SUGGESTION`.
- `CalcExpr.validateReferences` (PR #26) does the same for `calculated_measures:` — parses
  the CalcExpr DSL and checks every `Ref(name)` and `All(name)` against the visible
  measure set. Calc-of-calc chains validate in declaration order.
- Filter visibility tightened (PR #27) — `SparkFilterValidator` now sees transform outputs
  (a filter that references a transform output is no longer falsely rejected).
- 11 new tests in `VersionAndValidatorSpec` covering all four YAML blocks. 294 total
  library tests on both Spark 3.5.8 and 4.1.1.

### Done — `queryAs[T]: Dataset[T]` typed bundled query (v0.1.7)

- `queryAs[T](measures, dimensions, where, having, orderBy, limit, timeGrain, timeGrains, timeRange)`
  bundles the same parameters as `query(...)` and returns a `Dataset[T]`
  (Spark `Encoder`-flavored). The implicit `ResultDecoder[T]` decodes each
  row; an explicit `Encoder[T]` (normally `import spark.implicits._`) lets
  Spark materialize the typed collection.
- Compile-time safety: if the case class field names or types don't
  match the result schema, you get a compile error rather than a runtime
  `AnalysisException` or wrong values.
- 5 new tests in `QueryAsSpec.scala` cover the case-class path, the
  `where` filter, the `orderBy` + `limit` ordering, the full params
  round-trip, and a zero-grouping single-row query.
- 353 library tests pass (up from 341).

### What's NOT done from the original plan

- **E3 — typed arithmetic DSL** (`divide[N, D, R]` etc.): replaced by `ResultDecoder`-style
  result-typed calcs. Still deferred.

### What's explicitly still out of scope (unchanged from the table below)

- Type-safe Spark schema reading
- Generic `NumericExpr` with full arithmetic
- Implicit `Column → NumericExpr` conversions
- Breaking changes to existing APIs

```
Week 1, Day 1-2:  E1  ResultDecoder[T] + query[T]
Week 1, Day 3-4:  E2  Dimension[T] / Measure[T] phantom types
Week 2, Day 1-2:  E3  Typed arithmetic DSL (divide, multiply, etc.)
Week 2, Day 3-5:  Tests + regression suite
```

Total: **~2 weeks**. No API breaks. All new variants coexist with existing code.

---

## What's explicitly out of scope for Phase E

| Deferred | Reason |
|---|---|
| Type-safe Spark schema reading | Spark resolves schemas at runtime; any typeclass would be `Typeable[T]` backed by runtime reflection — adds complexity for marginal safety |
| Generic `NumericExpr` with full arithmetic | Scala's `Numeric` requires all numeric ops (`min`, `max`, `abs`) — complex to implement correctly, easy to misuse; the typed `divide` above is sufficient for the common case |
| Implicit Column→NumericExpr conversions | Conflicts with Spark's own `Column` implicits; too surprising; E3's explicit `divide[Long, Long, Double]` is clearer |
| BREAKING changes to existing APIs | Existing Dimension / Measure / Predicate are frozen. New typed variants are additive. |
| Type-safe predicate filtering | `"carrier" > 600` where carrier is String is a compile error only if carrier has a phantom type AND the predicate DSL infers the column type from the model. Possible but adds significant complexity; deferred to Phase F if needed. |

---

## Testing strategy

- E1: unit tests for `ResultDecoder.derive` with 3–4 case class shapes (correct types, wrong types, missing fields, extra fields)
- E2: compile-time tests (in a `src/test/scala/` that should compile — ScalaTest can't assert on compile errors directly, so use a script that asserts the compiler exit code)
- E3: property tests on typed arithmetic (identity laws, Spark output equivalence with untyped path)
- Regression: all 53 existing tests still pass — no breaking changes

---

## Exit criteria

- [ ] `query[T]` returns `Dataset[T]` with compile-time type safety on case class field names and types
- [ ] `Dimension[T]` / `Measure[T]` coexist with untyped variants — existing code unchanged
- [ ] Typed calc formulas (`divide[Long, Long, Double]`) compile and produce identical results to untyped equivalents
- [ ] All 53 existing tests green (no regressions)
- [ ] At least one synthetic test that **would have failed** at runtime without E1–E3 but **compiles** with them (e.g. a measure with a typo in the case class field name)
- [ ] New tests: E1 (4 tests), E2 (compile-time assertion), E3 (3 tests) — all green

---

## Risk log

| Risk | Likelihood | Mitigation |
|---|---|---|
| E3's typed arithmetic conflicts with Spark's Column implicits | Medium | Explicit function calls (`divide(...)` not `a / b`); keep Spark implicits working for untyped path |
| Phantom types add complexity without catching real bugs | Medium | Only do E1 + E2 unless first consumer has specifically hit typed-calc errors |
| ResultDecoder.derive breaks on complex case classes (nested, Option) | Low | Document supported shapes; manual instance for complex cases |
| Performance regression from Dataset vs DataFrame | None | Dataset[T] is just DataFrame with an encoder; no execution difference |

---

## Relationship to future phases

- **Phase F (typed predicates):** If E2/E3 are done and real consumers want `"carrier" > 600` to be a compile error, E2's phantom types provide the foundation. Deferred until real consumer demand.
- **Phase F (streaming):** Streaming terminal shipped separately from Phase E (PRs #110–#121). The streaming op-tree share the same builders as batch (`groupBy`, `aggregate`, etc.), but `query[T]: Dataset[T]` does not fit streaming's micro-batch model — `queryAs[T]` stays batch-only. Operators consume streaming results via the `foreachBatch` callback in `StreamingQueryOptions`.
- **Phase F (data catalog metadata):** Typed `Dimension` / `Measure` carry the phantom type alongside metadata. Non-blocking.
