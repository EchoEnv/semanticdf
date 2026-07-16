# Release notes

## v0.1.2 — lazy compile contract for `withTransforms`

A focused refactor that fixes a long-standing side effect in
`SemanticTable.withTransforms` on join models. Library and MCP server
are at `io.semanticdf:semanticdf_2.13:0.1.2` and
`io.semanticdf:semanticdf-mcp_2.13:0.1.2`.

### What changed

Before v0.1.2, `withTransforms` on a join model called
`j.compile(SparkSession.active)` eagerly to get the joined DataFrame, then
applied `withColumn` against it. Two problems:

1. **`SparkSession.active` is a side effect** — it auto-creates a default
   session if none is set. A consumer building a `SemanticTable` in a
   context without a session (config loading, `validate()` calls, catalog
   accessors like `dimensions`/`measures`/`joins`) would silently get a
   Spark session.
2. **Eager join compilation** broke the lazy compile contract
   (DESIGN §4.4). Every other op in the tree (`filter`, `orderBy`,
   `limit`, `hint`) is a passthrough that defers compilation to
   `toDataFrame(spark)`. `withTransforms` on a join was the only op
   that didn't.

After v0.1.2, `withTransforms` wraps the source in a new
`SemanticTransformsOp`. The transforms are applied lazily at
`toDataFrame(spark)` time, consistent with every other op. The op tree
holds no `SparkSession`, the compiled `DataFrame` is never stored in a
node, and `toDataFrame(spark)` is the only place that compiles.

### Public API: zero breaking changes

- `withTransforms` signature is unchanged
- Return type (`SemanticTable`) is unchanged
- Catalog accessors (`dimensions`, `measures`, `joins`, `filters`,
  `schema(spark)`, `findDimension`, `findMeasure`, etc.) return the
  same data
- `toDataFrame(spark)` produces a byte-identical `DataFrame` for the
  same query — verified by the 8 pre-existing `TransformsSpec` tests
  passing unchanged

### Chained transforms compose

Multiple `withTransforms` calls compose into a single
`SemanticTransformsOp` layer, applied in declaration order. The
earlier transforms are NOT replaced — they compose with the new ones.
This is the same `withColumn`-chain semantics you'd get in plain
Spark, just deferred.

```scala
st
  .withTransforms(Transform("a", t => t("v") + 1))   // applied first
  .withTransforms(Transform("b", t => t("a") * 2))   // applied second, sees `a`
```

### How this was built

5 PRs landed on top of v0.1.1, each small enough to review
independently:

- **#33** — scaffolded the new `SemanticTransformsOp` op node
  (pure addition, no behavior change)
- **#34** — added passthrough cases to all 20 tree walkers in
  `SemanticTable.scala` so the new op is recognized everywhere
  (pure addition, no behavior change)
- **#35** — flipped the switch: `withTransforms` now returns
  `SemanticTransformsOp` instead of eager-applying. The
  `applyTransforms` private helper was removed (its logic is now
  in `SemanticTransformsOp.compile`). One extra case was added to
  `SemanticAggregateOp.resolveModel` to unwrap the new op when
  finding the underlying model.
- **#36** — added the `LazyTransformsSpec` test suite (4 tests) AND
  fixed a chained-transforms bug that the new test caught: the
  recursion in the `SemanticTransformsOp` case was re-entering
  the match and dropping the existing transforms. Fix: don't
  recurse, just append.
- **#37** — documented the chaining behavior in the
  `withTransforms` scaladoc (17 lines).

### Why this matters

`withTransforms` is the bridge between the library and Spark
internals — it's how users add per-row derived columns
(`datediff`, `case when`, window functions). The eager-compile
side effect made it dangerous in library code paths that
shouldn't depend on Spark (config loading, `validate()` for CI
gates, catalog accessors for tooling). v0.1.2 makes it safe to
use `withTransforms` anywhere, with the same lazy contract as
every other op.

### Tests

- **298** library tests on both Spark 3.5.8 (default) and 4.1.1
  (was 294 in v0.1.1; +4 from `LazyTransformsSpec`)
- All 7 examples still build and run end-to-end
- **All 3 CI checks** (OKF drift, Spark 3.5.8, Spark 4.1.1) pass

### Files changed (cumulative across #33-#37)

- `src/main/scala/io/semanticdf/SemanticOp.scala` — adds
  `SemanticTransformsOp` case class; one extra unwrap case in
  `SemanticAggregateOp.resolveModel`
- `src/main/scala/io/semanticdf/SemanticTable.scala` — `withTransforms`
  switch flip; 20 passthrough cases in tree walkers; removes
  `applyTransforms` helper; rewritten scaladoc
- `src/test/scala/io/semanticdf/LazyTransformsSpec.scala` — **NEW
  FILE**, 4 tests verifying the lazy-transforms contract

### Migration

None required. Public API is unchanged. Any code that called
`withTransforms` on a join model will see identical query results
(verified by the 8 pre-existing `TransformsSpec` tests), but the
op tree is now lazy — the join isn't compiled until
`toDataFrame(spark)` is called. For the typical
`st.withTransforms(...).groupBy(...).aggregate(...).execute(spark)`
pattern, this is invisible.

### Out of scope for v0.1.2 (deferred to v0.2)

- `QUERY_TIMEOUT` / `RESULT_TOO_LARGE` MCP error codes
- `AMBIGUOUS_MEASURE` / `AMBIGUOUS_DIMENSION` (for multi-hop joins)
- `Introspector` warning lines in generated YAML
- `ResultDecoder[T]` (typed query *results*)
- **REST API** (Tier 3 plan) — JSON over HTTP, no JDBC driver needed
- Transform outputs in the catalog (so they can be referenced via
  typed refs) — separate, additive feature

All additive — they widen the contract without breaking any
existing agent.

### Verifying the release

```bash
# Library
mvn -o test                          # 298/298 on Spark 3.5.8
mvn -o test -Pspark4                 # 298/298 on Spark 4.1.1

# MCP server
cd semanticdf-mcp && mvn -o test    # 35/35

# OKF drift
make okfgen-check                   # bundle in sync with YAMLs
```

---

## v0.1.1 — type-safety + YAML load-time validation pass

A focused release that completes the type-safety story (PR #24) and the YAML
load-time validation pass (PRs #25–#27). Library and MCP server are at
`io.semanticdf:semanticdf_2.13:0.1.1` and
`io.semanticdf:semanticdf-mcp_2.13:0.1.1`.

### Highlights

- **Typed `withMeasures` + `SortKey.asc/desc` overloads** (#24) — accept the
  typeclass instance directly via subtyping. The typed overload is picked
  over the string overload even from cross-package consumer code (Scala
  2.13 phase-1 overload resolution matches by subtyping without needing
  an implicit conversion). The measure's name is read from the witness,
  not a string. Both overloads funnel through a private `withMeasures0`
  helper.
- **YAML load-time expression validation** (#25–#27) — every `expr` field
  in the YAML model schema now fails fast at `YamlLoader.load(...)` on
  unknown references. `ExpressionValidator` covers `dimensions:`,
  `transforms:`, and `measures:` (via Spark's `CatalystSqlParser`);
  `CalcExpr.validateReferences` covers `calculated_measures:` (via the
  CalcExpr DSL). A typo that previously surfaced as a cryptic Spark
  `UNRESOLVED_COLUMN.WITH_SUGGESTION` at first query time now fails at
  model-load with a clear message naming the model, the field, the missing
  identifier, and the visible column set.

### Full visibility rules (every `expr` field)

| Block | Validator | Visible identifiers |
|-------|-----------|---------------------|
| `dimensions:` | `ExpressionValidator` | source columns |
| `transforms:` | `ExpressionValidator` | source + previously-declared transforms |
| `measures:` | `ExpressionValidator` | source + transforms + previously-declared measures |
| `calculated_measures:` | `CalcExpr.validateReferences` | base measures + previously-declared calcs |
| `filters:` | `SparkFilterValidator` | source + transforms (joined-side columns still excluded — pre-join semantics) |

### Library surface (vs v0.1.0)

- New: `src/main/scala/io/semanticdf/ExpressionValidator.scala`
- Modified: `SemanticTable.scala` (typed `SortKey.asc/desc` + typed
  `withMeasures(measure, expr)` overloads)
- Modified: `CalcExpr.scala` (new `validateReferences` method)
- Modified: `YamlLoader.scala` (validation wiring; cumulative column set
  tracked across the `transforms:` and `measures:` blocks; calc loop is
  now a fold-left that tracks previously-declared calc names)
- 16 new tests in `VersionAndValidatorSpec` (7 for dims/transforms/
  measures, 4 for calcs, 1 for the filter-transform edge case, 1 for
  MixedCase columns, 3 for ordering correctness) and 4 new tests in
  `SemanticFieldSpec` (the typed `withMeasures` / `SortKey.asc(ref)`
  overloads).

### Tests

- **294** library tests on **both** Spark 3.5.8 (default) and 4.1.1
  (was 278 in v0.1.0).
- **35** MCP server tests (unchanged).
- All 7 example projects still build and run end-to-end. The hospital
  example's `age_years` calc (which used `year()` — outside CalcExpr's
  grammar) was removed as a pre-existing latent issue surfaced by the
  new validation; it was unused everywhere.

### Out of scope for v0.1.1 (still deferred)

- `QUERY_TIMEOUT` env-configured execution deadline
- `RESULT_TOO_LARGE` `MCP_MAX_ROWS` post-collect rejection
- `AMBIGUOUS_MEASURE` / `AMBIGUOUS_DIMENSION` (single-table scope today;
  multi-hop joins will need them)
- `Introspector` warning lines in the generated YAML (the hook is in
  place; the library just doesn't emit them yet)
- `ResultDecoder[T]` (typed query *results*, not just inputs)

All of these are additive — they widen the contract without breaking
any existing agent.

### Verifying the release

```bash
# Library
mvn -o test                          # 294/294 on Spark 3.5.8
mvn -o test -Pspark4                 # 294/294 on Spark 4.1.1

# MCP server
cd semanticdf-mcp && mvn -o test    # 35/35

# OKF drift
make okfgen-check                   # bundle in sync with YAML
```

### How this was built

4 PRs landed between 2026-07-15 and 2026-07-16 on top of v0.1.0:

- #24 — typed `withMeasures` + `SortKey.asc/desc` overloads
- #25 — `ExpressionValidator` for dims/transforms/measures
- #26 — `CalcExpr.validateReferences` for `calculated_measures:`
- #27 — Filter visibility: transforms visible to filters

---

# Release notes — v0.1.0

A stable, public-able cut of the library + the MCP server. Both modules share
the version (`io.semanticdf:semanticdf_2.13:0.1.0` and
`io.semanticdf:semanticdf-mcp_2.13:0.1.0`).

## Highlights

- **MCP server contract-complete** — all 5 tools from
  [`docs/agents/mcp-contract.md`](docs/agents/mcp-contract.md) ship:
  `list_models`, `describe_model`, `query`, `explain`, `introspect`. Sibling
  module under `semanticdf-mcp/`. Stdio transport via the official
  Model Context Protocol Java SDK (0.18.3).
- **Compile-time type safety** — `SemanticField` typeclass + typed
  `groupByDimensions` / `aggregateMeasures` overloads + typed `Predicate.Eq`
  / `Ne` / `Gt` / etc. factories. The `Carrier` / `TotalPax` / `Origin` phantom
  types catch measure/dimension swaps at compile time.
- **Sealed `Predicate.Compare` ADT** — `Eq` / `Ne` / `Lt` / `Le` / `Gt` / `Ge`
  are case classes inside the `Compare` companion; the legacy
  `Compare("op", field, value)` string factory is preserved for back-compat.
- **Pre-join row filters** — YAML `filters:` block + `withRowFilter(...)`
  Scala DSL. `SparkFilterValidator` enforces source-only / pre-join
  semantics at model-load time.
- **Per-model versioning** — `version: Int` field, `version(n: Int)`
  setter, YAML `version:` block, propagated through every tree walk.
- **OKF sidecar catalog** — `okfgen` produces per-model Markdown
  (`docs/agents/reference/<project>/<model>.md`); the MCP `describe_model`
  serves it inline via `okf_markdown`. `make okfgen-check` is the CI drift
  guard.

## Library surface

- 5 public top-level files: `Predicate.scala`, `Model.scala`,
  `SemanticTable.scala`, `SemanticOp.scala`, `YamlLoader.scala`.
- 278 tests on **both** Spark 3.5.8 (default) and 4.1.1.
- 7 example projects (`examples/`) — all build and run end-to-end.
- 3 CLI tools: `docsgen`, `introspect`, `okfgen`.

## MCP server surface

- New sibling module: `semanticdf-mcp/`.
- 5 tools registered; closed error-code list per the contract.
- Single shared `SparkSession` (lazy-init on first `query`/`explain` call).
- Stdio transport; logs to stderr; stdout reserved for JSON-RPC.
- 35 tests covering the handler logic (JSON predicate adapter,
  OrderBy adapter, request DTOs, error mapping, etc.).

## Out of scope for v0.1 (deferred to v0.2)

- `QUERY_TIMEOUT` env-configured execution deadline
- `RESULT_TOO_LARGE` `MCP_MAX_ROWS` post-collect rejection
- `AMBIGUOUS_MEASURE` / `AMBIGUOUS_DIMENSION` (single-table scope today;
  multi-hop joins will need them)
- `Introspector` warning lines in the generated YAML (the hook is in
  place; the library just doesn't emit them yet)

These are all additive — they widen the contract without breaking any
existing agent.

## Verifying the release

```bash
# Library
mvn -o test                          # 278/278 on Spark 3.5.8
mvn -o test -Pspark4                 # 278/278 on Spark 4.1.1

# MCP server
cd semanticdf-mcp && mvn -o test    # 35/35

# OKF drift
make okfgen-check                   # bundle in sync with YAML
```

## How this was built

22 PRs landed between 2026-06-11 and 2026-07-15. See `git log v0.1` for the
commit history. The first session focused on library features (typed
field refs, sealed Compare ADT, pre-join filters, per-model versioning,
catalog accessors for MCP); the second session wired the MCP server on
top of the now-stable library surface.
