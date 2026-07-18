# Release notes

## v0.1.5 — Scaladoc production-bar pass + style guide

A docs-only release. One PR since v0.1.4 (`#74`) that brings two
library files up to a higher Scaladoc bar and adds a contributor
style guide so the bar stays consistent going forward. No source
changes, no new tests, no behavior changes.

Library, MCP server, and CLI are at
`io.semanticdf:semanticdf_2.13:0.1.5`,
`io.semanticdf:semanticdf-mcp_2.13:0.1.5`, and
`io.semanticdf:semanticdf-cli_2.13:0.1.5`.

### Library

- **#74** — Scaladoc rewrite on `src/main/scala/io/semanticdf/Model.scala`
  and `src/main/scala/io/semanticdf/SemanticOp.scala`. The public API
  Scaladoc on `Dimension` / `Measure` / `Transform` / `MeasureExtra`
  was adequate but assumed the reader already knew the framework.
  Rewritten to a production bar with:
    - plain-language category openers ("A grouping field on a semantic
      model — the columns you `groupBy` on or filter against.")
    - copy-pasteable `{{{...}}}` examples on every factory, with all
      imports shown explicitly
    - actual exception types named, not "raises a clear error"
    - future / unshipped features qualified ("Reserved for the future
      streaming terminal (ADR 0002); no effect in batch today.")
    - a behaviour-accuracy fix on `Transform` — the old Scaladoc
      claimed transforms are "topologically sorted" (wrong). The
      implementation in `SemanticOp.scala:527` uses
      `transforms.foldLeft(base)(...)` — declaration order, no sort.
      README and `docs/guide.md` already said no sort; this brings
      `Model.scala` into line.

### MCP / CLI

No source changes — serverInfo / banner now report `0.1.5`.

### Docs

- **#74** — New `docs/scaladoc-style.md` (195 lines). The seven-rule
  guide for Scaladoc in `io.semanticdf.*`, with a do/don't section, a
  pre-commit checklist, and a "when NOT to update" section listing
  the three categories that should not get Scaladoc (examples, CLI
  tools, private members). `Model.scala` is the canonical example
  referenced throughout. `docs/DOCS_MAP.md` cross-links the new
  style guide from the by-journey and doc-roles tables.

### Test count

No test changes. **407 tests** (335 library + 72 MCP), all green on
Spark 3.5.8 (default) and Spark 4.1.1 (`-Pspark4`). Same as v0.1.4.

### Compatibility

No breaking changes. v0.1.5 is identical to v0.1.4 in compiled
behavior — only the Scaladoc strings changed.

## v0.1.4 — Introspect wiring, join-alias cleanup, ResultDecoder.derive, docs reorg

A docs-and-tooling release. Twelve PRs since v0.1.3 (#61–#72): two library
fixes (`Introspect` warnings end-to-end, clean `toJoinYaml` placeholder
names), one new macro (`ResultDecoder.derive[T]`), and the rest is the
four-tier documentation reorganization that produced `docs/guide.md`,
`docs/GLOSSARY.md`, `docs/DOCS_MAP.md`, an examples index, and a reframed
`docs/known-limitations.md`. Library, MCP server, and CLI are at
`io.semanticdf:semanticdf_2.13:0.1.4`,
`io.semanticdf:semanticdf-mcp_2.13:0.1.4`, and
`io.semanticdf:semanticdf-cli_2.13:0.1.4`.

### Library

- **#61** — `Introspect.parseInventory.skipped` now derives from
  `parseWarnings(yaml).length` instead of `totalCols - declared`, which
  under-counted when an entity column appeared in multiple YAML sections.
  New `IntrospectEndToEndSpec` (4 tests) drives a real CSV through
  `Introspect.handle()` via `SparkFixture`; new `introspect-busy.csv`
  fixture (12 cols).
- **#62** — New `FieldInfo.joinAlias(name)` helper strips entity suffixes
  (`_id`, `_key`, `_uuid`, `_code`, longest-first) + trailing underscore
  from the placeholder names `Introspector.toJoinYaml` emits. Fixes:
  `id` → `id_model` (was `_model`); `order_id` → `order_model` (was
  `order__model`); `event_uuid` → `event_model` (was `event_u_model`).
  +10 tests in `IntrospectorSpec`; new `join-alias-fixture.csv`.
- **#64** — `ResultDecoder.derive[T]` Scala 2 blackbox macro. Generates a
  decoder at compile time for case classes whose fields are all primitives
  (`String`, `Int`, `Long`, `Double`, `Float`, `Boolean`, `Short`, `Byte`,
  `java.math.BigDecimal`). Compile-time failure for non-case-classes and
  unsupported field types. Closes the typeclass-derivation deferral noted
  in `docs/phase-E-plan.md` §E1. New `ResultDecoderMacros.scala` (157
  lines); +6 tests.

### MCP server

- **#63** — Docs refresh (version/test-count references; orderBy
  limitation in `known-limitations.md` retired since fixed by #46).
- **#65** — Surface `ResultDecoder.derive[T]` in README + DESIGN +
  phase-E-plan + feature-roadmap docs.
- (serverInfo version string now reports `0.1.4`.)

### CLI consumer (`examples/cli-consumer/`)

- **#59** — Close out the "What building this surfaced" section in the CLI
  README: both findings (orderBy regression from #54, missing
  `exprString`) are now fixed (#56, #58). Section reframed as a
  regression-witness summary rather than an open findings list.
- (CLI banner now reports `0.1.4`.)

### Documentation reorg (four tiers)

The big lift this release. Each tier was one or two focused PRs.

**T1 — Glossary + Docs Map + README reorder (#67).** New
`docs/GLOSSARY.md` (~50 terms in 7 categories). New `docs/DOCS_MAP.md`
(3 wayfinding aids: by-journey table, doc-roles table, 3 reading paths
by time budget). README reordered: CLI Tools + MCP server moved right
after Quick start; Capabilities demoted to after the agent-facing
surfaces.

**T2 — examples index (#68).** New `examples/README.md` — central index
for the 8 example templates with a "Start here" command, 6 recommended
reading paths (evaluate/CRM/ops/messy-data/agent/telco), per-example
one-liners, and prerequisites.

**T3 — guide.md narrative + README trim (#69, #70).** New
`docs/guide.md` — the narrative walkthrough companion to `DESIGN.md`.
#69 landed the 11-section skeleton (~317 lines). #70 expanded every
section with an in-depth worked example (→ ~685 lines) and trimmed
README `## Capabilities` by relocating four tutorial-length subsections
(Transforms, Filters pre-join, Notebook escape hatch, EXPLAIN — combined
~400 lines) into `guide.md` as worked examples. README Capabilities went
from ~530 to 310 lines; each subsection is now a scannable teaser with a
link to its guide.md counterpart.

**T4 — per-doc audit + known-limitations reframe (#71).** Project-wide
staleness sweep across 24 hand-written docs: corrected Scala `2.13.14`
→ `2.13.18`, fixed a self-contradictory Spark-version table in README
(claimed `4.0.0` and `4.1.1` both via `-Pspark4`; actually default=
3.5.8, `-Pspark4`=4.1.1), refreshed test counts (329/401 → 335/407),
fixed 5 broken `examples/` links in `runtime-quickstart.md`. Reframed
`docs/known-limitations.md` from "Known Limitations" to "Current scope
& guardrails" — optimistic tone, "At a glance" summary table, each scope
item now carries Workaround-today + Roadmap labels, new Roadmap summary
closing section. All technical content (workarounds, code samples)
preserved verbatim.

### Repo hygiene

- **#66** — README intro rewrite: status banner replaced with an
  educational intro ("What problems SemanticDF solves" / "What you can
  do with it" / "When to use it" / "Where to read next").
- **#72** — Remove env-local `skills-lock.json` from the repo (was
  committed by accident before the `.gitignore` entry existed).

### Test count

- **Grand total:** 407 tests (335 library + 72 MCP), all green on Spark
  3.5.8 (default) and Spark 4.1.1 (`-Pspark4`). Up from 387 in v0.1.3
  (+16 library tests from #61 `IntrospectEndToEndSpec`, #62
  `IntrospectorSpec` join-alias, #64 `ResultDecoder.derive`; MCP
  unchanged at 72).

### Compatibility

No breaking changes. All v0.1.4 work is additive:
- `ResultDecoder.derive[T]` is a new macro alongside the existing manual
  `ResultDecoder[T]` construction.
- `FieldInfo.joinAlias` is a new helper; `Introspector.toJoinYaml`
  output changes (cleaner placeholder names) but the YAML it produces is
  consumed only by humans as a starter — no downstream code parses it.
- `Introspect.parseInventory.skipped` semantics changed (now derived from
  `parseWarnings(yaml).length`); the field's value is more accurate now,
  not less.

## v0.1.3 — Jackson Scala module + REST test infra + CLI consumer

A release that closes a regression from PR #54, fixes MCP-side test
isolation, introduces a real CLI consumer for the REST API, and surfaces
the original expression string in `describe_model`. Library, MCP server,
and CLI are at `io.semanticdf:semanticdf_2.13:0.1.3`,
`io.semanticdf:semanticdf-mcp_2.13:0.1.3`, and
`io.semanticdf:semanticdf_2.13:0.1.3` (cli-consumer module).

### What changed

**1. Real CLI consumer for the REST API** (#57). New `examples/cli-consumer/`
module — a standalone Scala binary (depends only on jackson-databind +
scala-library, no Spark, no semanticdf, no MCP SDK). Subcommands:
`list`, `describe`, `query`, `explain`. Parses responses via Jackson's
`JsonNode` tree model. `bin/sdf` wrapper caches the classpath
(`~0.5 s` startup after the first run). Proves the "any HTTP client can
drive the REST API" promise of the REST transport.

**2. Original expression string surfaces from `describe_model`** (#58).
Before v0.1.3, `DescribeModel` serialised `Dimension`/`Measure` `expr`
via the lambda's `toString`, producing opaque addresses like
`io.semanticdf.YamlLoader$$$Lambda$.../1234567`. Now `Dimension` and
`Measure` carry an optional `exprString` (defaults to `None`; populated
by `YamlLoader` from the YAML `expr:` value). `DescribeModel` prefers
the string. Joined dimensions surface the alias-prefixed user-facing
name (e.g. `carriers.name`).

**3. `OrderByParser` accepts both `java.util.Map` and Scala `Map`** (#56).
Regression fix from PR #54: with the Jackson Scala module registered,
nested objects deserialize as Scala `Map2`, but `OrderByParser.parse`
still pattern-matched `java.util.Map[_, _]`. Now matches both; explicit
error case for unsupported map types. 5 regression tests added.

**4. Shared `SparkSession` across MCP specs** (#55). New `SparkFixture`
trait in `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/`. One
`SparkSession` per test JVM (replaced per-spec `beforeAll`/`afterAll`
patterns that raced on `SparkContext.stop`). All MCP specs (`QuerySpec`,
`DescribeModelSpec`, `RestServerSpec`, `ListModelsSpec`) mix in.
`RestServerSpec`'s pre-existing `RESULT_TOO_LARGE` flake (masked by the
race) is now stable — 63/63 MCP tests pass deterministically.

**5. Jackson Scala module wired into MCP** (#54). `jackson-databind` is
pinned to `2.15.2` (Spark's bundled version); `jackson-module-scala_2.13`
is added as compile-scope. `JsonSupport.scala` provides a private[mcp]
mapper that registers `DefaultScalaModule` so `Envelope[T]` and all the
entry case classes (`DimensionEntry`, `MeasureEntry`, `JoinEntry`,
etc.) serialise as proper JSON objects instead of `{}`. Single consistent
Jackson version across the server.

**6. CLI README: "What building this surfaced" retired as a findings list**
(#59). Both surfaced findings (order_by regression, lambda expr
addresses) have been fixed in PRs #56 and #58. Section is now a 13-line
summary naming both PRs and framing `sdf` as a standing **regression
witness** for the REST contract. Section title kept so deep links don't
break.

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

### Public API: additive only, zero breaking changes

- New `Dimension.exprString` and `Measure.exprString` fields are optional
  with a default of `None`. Every existing `Dimension(...)` /
  `Measure(...)` call site compiles unchanged.
- New `examples/cli-consumer/` module is additive — it is *not* bundled
  with `semanticdf` or `semanticdf-mcp`. Existing consumers are untouched.
- `DescribeModel`'s JSON shape is unchanged (still `{dimensions: [...],
  measures: [...], ...}` with `expr: String` per entry); only the
  *value* of `expr` is now human-readable.
- `OrderByParser` accepts both map shapes — every legacy `java.util.Map`
  caller keeps working.

### Files changed (cumulative across #54–#59)

- `semanticdf-mcp/pom.xml` — Jackson version pin (`2.15.2`) +
  `jackson-module-scala_2.13` dependency.
- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/JsonSupport.scala`
  *(new)* — `scalaMapper()` factory registering `DefaultScalaModule`.
- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/Main.scala`,
  `RestServer.scala` — use `JsonSupport.scalaMapper()`.
- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/DescribeModel.scala`
  — serialise `exprString.getOrElse(expr.toString)`.
- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala`
  — `OrderByParser.parse` accepts Scala `Map` plus `java.util.Map`.
- `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/SparkFixture.scala`
  *(new)* — shared trait with `@transient private lazy val _spark`.
- `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/{Query,DescribeModel,RestServer,ListModels}Spec.scala`
  — mix in `SparkFixture`.
- `src/main/scala/io/semanticdf/Model.scala` — `Dimension` /
  `Measure` gain `exprString: Option[String]`; `Dimension.copy` /
  `equals` / `hashCode` updated.
- `src/main/scala/io/semanticdf/YamlLoader.scala` — populate
  `exprString` from the YAML `expr:` value in three builder sites
  (`buildDimension`, `buildBaseMeasure`, `buildCalcMeasure`) and the
  join re-exposure site.
- `src/test/scala/io/semanticdf/YamlLoaderSpec.scala` — 4 new tests
  locking expr-string preservation through YAML load.
- `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/handlers/DescribeModelSpec.scala`
  — 5 new tests (programmatic hint, back-compat fallback, complex
  expressions, entity/time dims).
- `examples/cli-consumer/` *(new module, all files new)* — pom.xml,
  `Main.scala`, `bin/sdf`, `bin/semanticdf`, README.md, .gitignore.
- `examples/cli-consumer/src/main/scala/com/example/sdfcli/Main.scala`
  — `maskExpr` docstring clarified as graceful-degradation hook.
- `examples/cli-consumer/README.md` — "What building this surfaced"
  retired as a findings list; example output refreshed with real
  expression strings.

### Tests

- **Library:** 319 (+4 for `exprString` preservation).
- **MCP:** 68 (+5 `describe_model` expr tests, +5 `OrderByParser`
  regression tests).
- **Grand total:** 387 tests, all green on Spark 3.5.8 and Spark 4.1.1.

### Migration

None required. To consume v0.1.3:

```scala
// build.sbt
"io.semanticdf" %% "semanticdf"     % "0.1.3",
"io.semanticdf" %% "semanticdf-mcp" % "0.1.3"
```

If you also want the standalone CLI consumer as a separate JAR:

```scala
"io.semanticdf" %% "semanticdf" % "0.1.3" classifier "cli-consumer"
// (or depend on `examples/cli-consumer/` directly from the source tree)
```

### Out of scope for v0.1.3 (deferred to v0.2+)

- REST auth / streaming (currently local-only by design)
- Cross-`SparkSession` model sharing / remote warehouse federation
- `Introspector` warning lines in emitted YAML (the suppression hook
  on the MCP side is in place; the library just doesn't emit them yet)

### Verifying the release

```bash
# Library
mvn -o test                          # 319/319 on Spark 3.5.8
mvn -o test -Dspark.version=4.1.1   # 319/319 on Spark 4.1.1

# MCP server
cd semanticdf-mcp && mvn -o test    # 68/68

# CLI consumer (smoke)
cd examples/cli-consumer
./bin/sdf list --url http://localhost:8080
./bin/sdf describe --url http://localhost:8080 <model-name>

# OKF drift
make okfgen-check                   # bundle in sync with YAML
```

### How this was built

Six PRs landed between 2026-07-15 and 2026-07-17 (#54–#59). See
`git log v0.1.2..v0.1.3` for the commit history. The cluster is split
between infrastructure wiring (Jackson Scala module, shared test
fixture), a regression fix (`OrderByParser`), and two surface additions
that close out the CLI consumer's first round of friction-finding
(real `expr` strings, and the CLI README itself).

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
- `ResultDecoder[T]` `Dataset[T]`-flavored `query[T]` shape
  (the `Seq[T]` via `collectAs[T]` path shipped in #52; the
  `Dataset[T]`-shaped variant is await-consumer-demand)

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
