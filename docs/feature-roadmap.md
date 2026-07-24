# Feature Roadmap & Performance Plan

**Status:** Living document — revised as features ship. Tier assignments reflect *current* gating, not original intent.
**Last updated:** v0.1.16 shipped (structured predicate on the MCP wire + dbt `manifest.json` reader). Audit log + result cache + perf/leak tests + Ossie adapter added in follow-ups. See [RELEASE.md](RELEASE.md) for the cumulative changelog. Pre-v0.1.16 entries below are kept for design history; the status markers on each item reflect its *current* gating. 8 templates shipping (`cli-consumer` added in v0.1.3); `sdf` CLI is the project's first real consumer.

This plan lists the features and performance improvements that would benefit semanticdf, organized by tier and gated on real consumer feedback. It does **not** commit to a timeline — every feature here should be re-evaluated after we have a first consumer.

---

## How to read this plan

Each feature has:
- **Tier** — priority bucket (T1 = ship next, T2 = ship when needed, T3 = nice-to-have, T4 = enterprise)
- **Effort** — rough estimate in person-days or weeks
- **Impact** — high / medium / low
- **Why this tier** — honest reasoning
- **Consumer gate** — what real-world signal would trigger building it

The plan deliberately avoids assigning calendar dates. Until we have a first consumer, dates would be guessing. Re-evaluate after each consumer conversation.

---

## Tier 1: Ship next (high impact, low effort)

These solve known universal pain and don't require consumer validation.

### 1.1 Lazy measure evaluation (column pruning)

**Status:** ✅ **SHIPPED** (commit a2a4b06) — infrastructure was already in place (`compileWithBase` already filtered by `measuresToCompute`); added 8 regression tests that lock in the behavior. Microbenchmark: **2.09x speedup** (167ms → 80ms) on 10-measure wide model when requesting 1 vs 10 measures.

**Problem:** `groupBy("carrier").aggregate("total_revenue", "avg_passengers")` evaluates BOTH measures in the Spark plan, even though only two were requested. With 20+ measures per model, this wastes cycles on every query.

**Solution:** Only compile and execute the measures the caller asked for. The current code evaluates all measures in `mergedModel.measures`; we should filter to `measureNames` before compiling.

**Effort:** 2-3 person-days
**Impact:** High — 30-50% faster queries on wide models (10+ measures).

**Files to change:**
- `src/main/scala/io/semanticdf/SemanticOp.scala` — `SemanticAggregateOp.compileWithBase` filters by `measureNames`
- Add a test that confirms unrequested measures aren't in the Spark plan

**Why T1:** Universal pain — every wide-model query is affected. No consumer signal needed.

**What shipped:**
- `ColumnPruningSpec.scala` — 8 regression tests that assert which aggregation functions appear in the Spark plan for various request shapes (1 measure, 2 measures, calc with transitive deps, calc + unrelated measure, empty measures, non-existent measure, result schema correctness).

---

### 1.2 Auto-generate YAML from a DataFrame

**Status:** ✅ **SHIPPED** (commit 31dd61f)

**Problem:** A new user has a parquet table and wants a semantic model. Today they must hand-write the YAML from scratch — listing every column, writing Spark SQL expressions, picking aggregation types. This is the #1 onboarding friction.

**Solution:** A CLI tool: `mvn exec:java -Dexec.mainClass=io.semanticdf.tools.Main -Dexec.args="introspect <path>"` reads a parquet/csv, infers:
- Dimensions: all string/timestamp/date columns → dims, with `is_time_dimension` set on timestamp/date
- Measures: numeric columns → base measures with `sum()`/`count()`/`avg()` suggestions
- Metadata: column-name-based heuristics (`email` → pii, `id` → identifier, `_at`/`_date` → time dimension)

Emits a starter YAML file the user can edit.

**What shipped:**
- `Introspector.scala` — `Introspector` class with full heuristics: `fromFile()` reads any Spark format, `toYaml()` produces the YAML string, configurable `maxMeasures` (default 8) to avoid overwhelming users with wide tables
- `Main.scala` — CLI with subcommands: `--path`, `--format`, `--model`, `--max-measures`, `--sample-size`, `--out`
- `pom.xml` — `exec-maven-plugin` with JVM `--add-opens` flags for Java 17+
- `IntrospectorSpec.scala` — 10 regression tests: string dims, numeric measures, entity dims (id/_id/_key), PII detection (email/phone/ssn), timestamp → time dimension, maxMeasures limiting, mixed-schema flights table, CSV fromFile, empty model name validation, flights demo output

**Why T1:** Every new user hits this. Universal.

---

### 1.3 Better error messages with "did you mean"

**Status:** ✅ **SHIPPED** (commit bbd766a)

**Problem:** Typo in a measure name: `aggregate("total_revneue")` → throws `IllegalArgumentException: Unknown measure 'total_revneue'`. The user has to scroll through the YAML to find the correct name.

**What shipped:**
- Extracted `closestMatch` (Levenshtein, threshold ≤ 3) to `package.scala` — shared across all error sites
- `SemanticOp.scala`: `"Unknown measure '$n'."` → `"Unknown measure '$n'. Did you mean: '$c'?"`
- `SemanticTable.scala`: `"atTimeGrain: dimension '$dimName' not found"` → adds "Did you mean" suggestion
- `Scope.scala`: the pre-existing calc-layer `UnknownFieldError` already had "Did you mean" via the shared `closestMatch`
- **Bug fix (incidental):** Measures with column typos (e.g. `t("flight_cont")`) were misclassified as base measures. Now caught in Pass 1 and retried via `MeasureScope` in Pass 2, surfacing the suggestion correctly

**Tests:** 4 new tests in `HardeningSpec.scala` (unknown measure suggestion, close-but-wrong names, nothing-close guard, atTimeGrain dimension suggestion). the pre-existing calc-typo test now passes. **278/278 total.**

**Effort:** 2-3 person-days
**Impact:** Medium — every error path benefits.

**Why T1:** Universal — every error message improves. Cheap.

---

### 1.4 Auto-generate HTML/Markdown documentation from YAML ✅ (v0.1.0)

**Done.** `io.semanticdf.tools.DocsGen` reads YAML model files and emits a self-contained HTML page — no external CSS, no templates:

```scala
val docsGen = new DocsGen()
docsGen.write("docs/index.html", docsGen.fromFile("models/"))
```

Output: sidebar nav (all models), per-model cards with dimension/measure/join tables, time/entity/pii badges, embedded CSS. CLI: `mvn exec:java -Dexec.mainClass=io.semanticdf.tools.Main -Dexec.args="docsgen --path models/"`.

---

### 1.5 EXPLAIN that shows semantic intent ✅ (v0.1.0)

**Done.** `SemanticTable.explainSemantic(spark)` produces a multi-section plan
designed to be readable by humans and useful for LLM agents debugging query plans.
Sections use light-box-drawing separators and plain-language explanations:

```
PLAN SUMMARY
────────────
  table:   flights
  group by: carrier
  compute:  total_passengers
  filters: 2 applied

──────────────────────────────────────────────────
SEMANTIC ROUTING
────────────────
  Why each filter goes WHERE (before agg) vs HAVING (after agg).
  Think: WHERE = fast pre-aggregation; HAVING = post-aggregation only.

  HAVING → total_passengers > 100
      └─ runs after aggregation (slower); touches: total_passengers
  WHERE  → carrier = AA
      └─ runs before aggregation (fast); touches: carrier

──────────────────────────────────────────────────
TRANSITIVE DEPENDENCIES
───────────────────────
  Measures computed (requested directly or by other calc measures).
  Column pruning means Spark only reads the columns it actually needs.

  Will compute: total_passengers
  Skipped (not needed): flight_count
  Spark will not compute these — column pruning skips them

──────────────────────────────────────────────────
DIMENSIONS (1)
──────────────
  Columns you can group by, filter on, or use in orderBy.

  carrier

──────────────────────────────────────────────────
MEASURES (1)
────────────
  Aggregations: base = direct agg; calc = built from other measures.

  total_passengers  [base]

──────────────────────────────────────────────────
JOINS (0)
─────────
  (none)

──────────────────────────────────────────────────
SPARK PLAN (df.explain)
───────────────────────
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Filter (isnotnull(total_passengers#L) AND (total_passengers#L > 100))
     +- HashAggregate(keys=[carrier#X], functions=[sum(passengers#Y)])
        ...
```

**Design principles applied:**
- **Plain language over jargon.** `runs before aggregation (fast)` instead of `pre-agg`.
- **Tree-drawing glyphs** (`└─`, `→`) to show hierarchy and direction.
- **Light separators** (`─` × 50) instead of heavy 80-char `===` lines.
- **Per-section one-liner explainer** so first-time readers know what each section is for.
- **Joins show all involved tables**: `table: orders + customers` instead of just one.
- **Aggregate info surfaced even when wrapped** (e.g. by HAVING) — dig through filters.
- **Calc-vs-base classification uses a real probe** (`MeasureProbeScope`) that records
  measure references without executing; replaces the previous broken string-match heuristic.

**Files changed:**
- `src/main/scala/io/semanticdf/SemanticTable.scala` — added `explainSemantic`, private
  `SemanticPlanRenderer`, `MeasureProbeScope`, plus refactors to renderers (~520 LOC).
- `src/test/scala/io/semanticdf/ExplainSemanticSpec.scala` — 8 regression tests.

**Effort:** ~1.5 person-days (rendering + polish after first user feedback).

```
PLAN for orders.groupBy("carrier").where("status === 'shipped'").aggregate("total_revenue"):

ROUTING:
  filter status === 'shipped'      → WHERE (pre-agg)     [reason: status is a dimension]
  groupBy carrier                 → group-by key
  aggregate total_revenue         → sum(total_amount)

SPARK PLAN:
  Scan parquet (28 MB scanned, 0 partitions pruned)
  Filter (status = shipped)             ← pushed to scan via PushedFilters
  HashAggregate (carrier, sum)          ← no exchange (single partition)
  Output (2 rows)

ESTIMATED: scans 28 MB, no shuffle
```

---

### 1.6 SQL-mode CLI for ad-hoc exploration

**Status:** ✅ **SHIPPED** (v0.1.15)

**Problem:** The first time someone tries SemanticDF they have to read the Scala DSL, write a `Main.scala`, wire up a build, and run via `mvn scala:run`. That's the #1 onboarding friction after the first compile. SQL is the universal language every analyst knows.

**Solution:** A `query` subcommand that takes a SQL string and runs it against a YAML model. No Scala code required.

```bash
mvn exec:java \
  -Dexec.args="query --models examples/starter/models/ \
  --sql 'SELECT carrier, total_passengers FROM flights \
  GROUP BY carrier ORDER BY total_passengers DESC LIMIT 10'"
```

**What shipped:**
- `SqlCli.scala` — ~340 LOC tokenizer + recursive-descent parser. Handles `SELECT [item [AS alias], ...] FROM model [WHERE cond] [ORDER BY ord] [LIMIT n]`. `WHERE` supports AND/OR/+nested parens. `ORDER BY` supports ASC/DESC. `GROUP BY` is accepted in any position (the model decides grouping from the SELECT items). Numbers, strings (`'...'` with `''` escape), and identifiers. Rejects unknown fields with the same "Dims: ... Measures: ..." error format used elsewhere.
- `Main.scala` — `runQuery` subcommand. `--models <dir-or-file> --sql '<sql>'`. Prints result via `DataFrame.show(truncate=false)`.
- `SqlCliSpec.scala` — 16 unit tests covering every grammar rule and error path.
- `SqlCliEndToEndSpec.scala` — 4 end-to-end tests with a real Spark session and a YAML fixture.
- `runtime-quickstart.md` — usage example.

**Tests:** 20 new tests (16 unit + 4 e2e). 566/566 total.

**Why T1:** Universal — zero-code exploration. The single biggest consumer-friction win.

---

### 1.7 Structured predicate on the MCP query wire

**Status:** ✅ **SHIPPED** (v0.1.16)

**Problem:** The MCP `query` wire shape uses a flat `where: [predicates]` array — the server has to AND-combine them with `Predicate.And(...)` on the agent's behalf. Agents that want to author a single composable tree (nested AND/OR) have to flatten their intent into a list. Plus, the wire shape doesn't match the library's `PredicateAst` (used by the joined-manifest wire in v0.1.13), so two parts of the system express the same concept in two different shapes.

**Solution:** An alternative `ast_where` (and `ast_having`) field on the `query` and `explain` tools. The op set mirrors the library's `PredicateAst` exactly: `eq` / `neq` / `lt` / `lte` / `gt` / `gte` / `and` / `or`. Both fields can be present; the server AND-combines them.

```json
{
  "model": "flights",
  "measures": ["flight_count"],
  "ast_where": {
    "op": "and",
    "left":  {"op": "gt",  "left": "distance", "right": 500},
    "right": {
      "op": "or",
      "left":  {"op": "eq", "left": "carrier", "right": "AA"},
      "right": {"op": "eq", "left": "carrier", "right": "UA"}
    }
  }
}
```

**What shipped:**
- `AstPredicates.scala` — ~80 LOC parser. Detects nodes vs values by the presence of an `op` key.
- `QueryRequest` — new `ast_where` / `ast_having` fields (additive — `where` / `having` keep working).
- `Query.mergedWhere` / `mergedHaving` — `private[handlers]` helpers for direct unit testing.
- `queryToolSchema` — two new properties.
- 16 unit tests (`AstPredicatesSpec`) + 7 integration tests (`QuerySpec`).
- `docs/agents/mcp-contract.md` — new §"Alternative: ast_where / ast_having", updated error-codes table, status v4.

**Why T1:** Closes a real ergonomics gap. Two parts of the system (library wire + MCP wire) now express predicates in the same shape.

---

### 1.8 dbt manifest reader

**Status:** ✅ **SHIPPED** (v0.1.16)

**Problem:** dbt users already maintain a manifest for their warehouse. They don't want to hand-author a second YAML to expose the same models to a semantic layer. Two sources of truth, twice the maintenance, twice the drift.

**Solution:** A Scala adapter (`DbtManifestReader`) that reads dbt's `manifest.json` (v12+, the format `dbt parse` produces) and turns it into a `Map[String, SemanticTable]`.

```scala
// Phase 1: read the manifest (pure, no Spark).
val project = DbtManifestReader.read(Paths.get("target/manifest.json"))

// Phase 2: bind to a Spark session via a caller-supplied resolver.
val tables = DbtManifestReader.toSemanticTables(project, spark, sourceTable =>
  spark.read.format("parquet").load(s"/data/$sourceTable"))
```

**Wire convention:** A column is a **dimension** by default. To mark a column as a **measure**, the user adds to their dbt `schema.yml`:

```yaml
columns:
  - name: total_revenue
    meta:
      kind: measure
      expr: "sum(amount)"
```

The reader checks for `meta.kind == "measure"` AND a non-empty `meta.expr`. Anything else stays a dimension — no `kind: dimension` marker (dimensions are the default).

**What shipped:**
- `DbtManifestReader.scala` — ~290 LOC. Two-phase API: `read(manifestPath)` / `read(manifest: Map)` for parse-only; `toSemanticTables(project, spark, resolve)` for Spark binding.
- Source-table resolution: `<database>.<schema>.<alias>` / `<schema>.<alias>` / `<alias>`. Caller controls how to interpret the string.
- 13 tests in `DbtManifestReaderSpec` covering manifest parsing, column partition, source-table formatting, end-to-end Spark binding, and error paths.
- `examples/dbt-reader/` — runnable demo: hand-crafted `manifest.json` + CSVs + `Main.scala`.
- `docs/design/dbt-manifest-reader.md` — design notes.

**What's NOT in v1 (deliberate, scope-limited):**
- **Joins.** dbt doesn't record join keys in the manifest. v1 emits the model graph without joins; users add them via the existing `join_one` / `join_many` API.
- **Sources / metrics / streaming.** Sources are preserved in `DbtProject.sources` for v2; metrics in `rawNodes`; streaming isn't a dbt concept.

**Why T1:** Closes a real adoption gap. Every dbt user with a manifest is a potential SemanticDF consumer.

---

### 1.9 Audit log

**Status:** ✅ **SHIPPED** (post-v0.1.16)

**Problem:** LLM agents running on top of the semantic layer make queries that humans don't review line by line. Without an audit trail, you can't answer: what did my agent just query? Is the same query being run repeatedly (cache candidate)? Did the last run hit a timeout? How long did the query take, and how many rows came back?

**Solution:** A minimal, pluggable audit primitive in `io.semanticdf.audit.*` — an `AuditEvent` case class, an `AuditSink` trait, a `PredicateHasher` for stable filter fingerprints, and `SemanticTable.withAuditSink(...)` to opt in.

```scala
val t = toSemanticTable(df, name = Some("flights"))
  .withDimensions(...)
  .withMeasures(...)
  .withAuditSink(AuditSink.JsonlStdout)  // opt-in

t.query(measures = ..., dimensions = ..., where = ...).toDataFrame(spark)
// emits: {"ts":"...","model":"flights","measures":[...],"dimensions":[...],
//         "where_hash":"<sha256>","having_hash":null,"row_count":0,
//         "elapsed_ms":42,"status":"ok"}
```

**What shipped:**
- `audit/AuditEvent.scala` — the case class (12 fields, all optional/defaulted)
- `audit/AuditSink.scala` — the trait + 3 default impls: `NoOp`, `JsonlStdout`, `inMemory(maxEvents)`
- `audit/PredicateHasher.scala` — stable SHA-256 of a `Predicate` tree. Commutative for And/Or. Independent of construction order. Same canonical form for the library `Predicate` and the v0.1.16 MCP `PredicateAst`.
- `audit/QueryRequest.scala` — the captured request shape (model, measures, dimensions, where, having)
- `SemanticTable` — two new constructor fields (`auditSink`, `auditRequest`), one fluent setter (`withAuditSink`), one internal `copyAuditRequest`. The audit fields are preserved across the chainable methods (where, having, orderBy, limit, groupBy, aggregate, join_*).
- `toDataFrame()` — emits the event on success or failure. No-op fast path when no sink is set (zero overhead).
- 19 tests in `AuditSpec`: hasher (10), sink (4), end-to-end (5).
- `docs/design/audit-log.md` — design notes.

**Why this is the right T2 anchor:** Every other T2 item benefits from it. Cache invalidation reads the audit stream. Tracing hooks off it. Debugging uses it. The `whereHash` / `havingHash` are the seed for cache-key equivalence by AST shape — the natural follow-up to v0.1.16's predicate AST work.

**What's NOT in v1 (deliberate):**
- Eager row count — would force `df.count()` (re-run the plan). The field is reserved (default `0`); consumers extend.
- MCP `audit_log` retrieval tool — the sink is in place; the tool is a separate PR.
- Async / queue-based sinks — premature for the current call rate.

---

### 1.10 MCP `audit_log` retrieval tool (follow-up to 1.9)

**Status:** ✅ **SHIPPED** (v0.1.17, follow-up to v0.1.16 audit log)

**Problem:** The v0.1.16 audit log emitted events into a sink, but agents had no way to read them back. Self-introspection ("what did I just query?") and run-diffs required a separate observability stack.

**Solution:** A sixth MCP tool `audit_log` that reads the in-memory audit buffer and returns the recent events in arrival order.

```json
{ "limit": 100 }     // optional, default 100, capped at 1000
```

Response:
```json
{
  "events": [ /* AuditEvent JSON, oldest first */ ],
  "count": <int>, "total": <int>, "truncated": <bool>
}
```

**What shipped:**
- `AuditSink.snapshot()` — default `Seq.empty`; `InMemoryAuditSink` overrides to return the buffer. Non-breaking (default impl).
- `handlers/AuditLog.scala` — ~150 LOC handler + wire schema. Reads from a shared `InMemoryAuditSink` (1024-event ring buffer).
- `Query` handler — new `auditSink: Option[AuditSink]` constructor param. When set, calls `t.withAuditSink(sink)` on the resolved model before `query()`.
- `Server.scala` — wires one shared `InMemoryAuditSink` to both the `Query` handler (writer) and the new `AuditLog` handler (reader). Registered as the sixth tool.
- 12 new tests in `AuditLogSpec` (10 unit, 2 integration with `Query`).
- `docs/agents/mcp-contract.md` — status bumped to v5, new "Tool 6: `audit_log`" section.

**Privacy:** events carry the request shape but **not** the filter's literal values. `where_hash` is a stable SHA-256 of the canonicalized `Predicate` tree; equivalent filters hash to the same value.

**Why this is the right tight-loop:** The audit sink was built in v0.1.16 specifically to support a retrieval tool. One small PR closes the loop; nothing in v0.1.16 needs to change. The T2 cache work (#1.11 candidate) can now use `whereHash` as the cache key.

---

### 1.11 Result cache

**Status:** ✅ **SHIPPED** (post-v0.1.16)

**Problem:** LLM-agent loops re-ask the same question while reasoning, and several agents in the same session hit the same semantic table. Without a result cache, every query is a fresh Spark job — the second identical query costs as much as the first.

**Solution:** A minimal, pluggable cache primitive in `io.semanticdf.cache.*` — a `ResultCache` trait with a default LRU `inMemory` impl, a `CachedResult` (rows + schema) value type, and a `CacheKey` (SHA-256 of the canonicalised request shape) key type. `SemanticTable.withResultCache(...)` opts in.

```scala
val t = toSemanticTable(df, name = Some("flights"))
  .withDimensions(...).withMeasures(...)
  .withResultCache(ResultCache.inMemory(maxEntries = 256))

t.query(measures = ..., dimensions = ..., where = ...).toDataFrame(spark)
// 1st call: executes, stores. 2nd call: cache hit, no Spark job.
```

**What shipped:**
- `cache/ResultCache.scala` — the trait + `NoOp` and `inMemory(maxEntries)` factories.
- `cache/InMemoryResultCache.scala` — LRU cache, ~30 LOC.
- `cache/CachedResult.scala` — `Array[Row] + StructType` value type.
- `cache/CacheKey.scala` — SHA-256 of `m=<model>|me=<sorted measures>|dim=<sorted dimensions>|w=<whereHash>|h=<havingHash>`.
- `SemanticTable` — 1 new constructor field (`resultCache`), 1 fluent setter (`withResultCache`). The `toDataFrame` audit path extended to check the cache first, fall through to execute on miss, and store the result. Zero-overhead fast path when no audit sink AND no cache is set.
- 19 tests in `ResultCacheSpec`: 8 key, 4 sink, 7 end-to-end (including the audit + cache interaction test).
- `docs/design/result-cache.md` — design notes.

**Performance:**
- **Cache hit:** O(rows) to rebuild a DataFrame from cached rows. No Spark plan, no collect, no job. The "best performance" path.
- **Cache miss:** at least one Spark job (collect), then one `put`. The win is on repeated queries.

**Why this is the right T2 anchor:** the `whereHash` from v0.1.16's audit log is the seed for cache-key equivalence by AST shape. The cache uses the same `PredicateHasher` — so audit-log entries and cache keys are always consistent. v0.1.16 + 1.11 (this) closes the loop: log every query, then cache the repeats.

**What's NOT in v1 (deliberate, per karpathy "no speculative features"):**
- TTL / invalidation hooks — a follow-up if real workloads need it.
- Cross-process durability — the cache is in-memory only.
- Per-row invalidation — keyed by request, not by source rows.
- `orderBy` / `limit` / `timeGrain` / `timeRange` in the key — a follow-up if false cache hits show up in practice.

**Interaction with the audit log:** the two features share `auditRequest`. On a cache hit, the audit event's `rowCount` is the cached row count and `elapsedMs` is small (no compile, no collect). An `mcp audit_log` consumer can detect "this was a cache hit" by `elapsedMs < 1ms` and `rowCount > 0`.

---

### 1.12 Performance baseline + leak tests

**Status:** ✅ **SHIPPED** (post-v0.1.16)

**Problem:** The post-v0.1.16 wave shipped three features with new resource ownership semantics (audit buffer, cache buffer, audit-event flow). Without tests, the "no overhead, no leak, best performance" contract is unverified — leaks in particular can sit for months before users notice.

**Solution:** Two new test packages — `io.semanticdf.leak` (gates: real failures mean real bugs) and `io.semanticdf.perf` (observational: published numbers, not gates).

**Leak tests (8 tests, gates):**
- `InMemoryAuditSink` buffer bounded by `maxEvents` — emit 10k events, assert the buffer holds at the cap
- `InMemoryResultCache` buffer bounded by `maxEntries` (LRU eviction) — insert 200 entries with cap=100, assert 100 keys
- `clear()` empties the buffer
- `PredicateHasher` is stateless under concurrent access — 8 threads × 1k hashes
- `toDataFrame` chain survives 100 calls without plan accumulation
- Cache under load: 100 distinct queries with cap=4 → exactly 4 keys retained

**Perf tests (6 tests, observational):**
- `toDataFrame` on a 30-row table (the basic terminal)
- Cache miss vs cache hit (validates "best performance")
- Cache hit + audit sink (realistic LLM-agent path)
- Predicate hashing throughput (small/medium/large)
- Full cache hit end-to-end (key compute + lookup + rebuild)
- 200 puts with eviction (LRU throughput under load)

Each test publishes a median via `info(...)`; numbers land in `target/surefire-reports/semanticdf-test-suite.txt` with the `[perf]` prefix for trend-watching.

**v0.1.17 baseline** (Spark 3.5.8, JDK 17, single-machine local mode):
- `toDataFrame`: 62ms median
- Cache hit: 26ms (vs 62ms miss — **2.4× faster**)
- Cache hit + audit: 28ms (audit is sub-millisecond)
- Predicate hash: 20µs small, 22µs large (1k iterations)
- LRU puts: sub-millisecond

**Why this is the right time:** the audit log + result cache are the most likely places to leak (in-memory buffers, thread-safety, plan retention). Baselining them now means future T2 features can be measured against this anchor. The user-facing requirement is "no overhead, no leak, best performance" — these tests make that contract testable.

**What we DON'T do:** perf tests are **not gates**. A slow CI day doesn't block a PR. The value is trend-spotting, not per-PR enforcement. Leak tests are gates (real bugs deserve real failures).

**Files:**
- `src/test/scala/io/semanticdf/leak/LeakSpec.scala` (new, 8 tests)
- `src/test/scala/io/semanticdf/perf/PerfBaselineSpec.scala` (new, 6 tests)
- `src/main/scala/io/semanticdf/cache/ResultCache.scala` (added `keys()` + `clear()` to the trait with default impl)
- `docs/design/perf-baseline.md` (new design doc with the v0.1.17 numbers)

**Library: 631/631 pass** (was 617, +14 tests; no regressions).

---

### 1.13 Apache Ossie adapter + typeclass interface

**Status:** ✅ **SHIPPED** (post-v0.1.16)

**Problem:** The semantic layer ecosystem has multiple incompatible metadata formats — dbt's `manifest.json`, Apache Ossie (formerly OSI), Cube.js, LookML, internal formats. Every new format means every tool that consumes a semantic layer has to write a new adapter. The library shouldn't have to pick a winner.

**Solution:** A typeclass (`SemanticMetadataAdapter[Source, Project]`) that unifies the two-phase API (parse + bind) and provides a unified `loadSemanticTables(...)` entry point. Two instances ship: `DbtAdapter` (wraps the existing `DbtManifestReader`) and `OssieReader` (new, parses Apache Ossie YAML).

```scala
import io.semanticdf.adapters.DbtAdapter
import io.semanticdf.adapters.OssieReader
import io.semanticdf.adapters.SemanticMetadataAdapter.loadSemanticTables

val dbtTables = loadSemanticTables(Paths.get("manifest.json"), spark, resolve)
val ossieTables = loadSemanticTables(Paths.get("flights.yaml"), spark, resolve)
```

**What shipped:**
- `SemanticMetadataAdapter.scala` — the trait + `loadSemanticTables` entry point. ~80 LOC.
- `DbtAdapter.scala` — thin wrapper over `DbtManifestReader`. ~40 LOC.
- `OssieReader.scala` — new reader for the Apache Ossie YAML format. ~330 LOC.
- `OssieProject.scala` — case-class DTO for the parsed Ossie structure.
- 10 new tests in `SemanticMetadataAdapterSpec` covering parse, toSemanticTables, end-to-end query, both adapters via the unified entry point, and error paths.
- `docs/design/ossie-adapter.md` — design notes + Ossie wire shape excerpt.

**Why this matters:** the existing `DbtManifestReader` works fine in isolation; the new value is **unification**. Future formats (Cube, Looker, Snowflake) plug in as new `SemanticMetadataAdapter` instances and inherit the entry point. The cost is small (~30 LOC for the trait), the win is "any future format works the same way."

**Apache Ossie status check:** the project is real, recently rebranded from "Open Semantic Interchange (OSI)" to "Apache Ossie" (commit `5ca32ad`). 10 vendor converters already exist (dbt, gooddata, polaris, snowflake, databricks, omni, honeydew, salesforce, orionbelt, wisdom). The canonical spec is at `core-spec/spec.yaml` with a JSON Schema at `core-spec/osi-schema.json` (version `0.2.0.dev0`).

**What we do NOT consume in v1:** other dialects (Snowflake/Databricks/BigQuery — picked on read), `ai_context` mapping (preserved on intermediate), the `ontology` block (preserved on the project), composite join keys (v1 picks the first column), `primary_key` / `unique_keys` (preserved on the intermediate, semanticdf doesn't have a first-class grain concept yet).

**Library: 641/641 pass** (was 631, +10 OssieReader tests; no regressions).

---

## Tier 2: Ship when consumer needs it

These are valuable but require real consumer pain to justify.

### 2.1 Query result caching

**Problem:** BI dashboards hit the same query every refresh (1 min, 5 min, etc.). Each refresh re-scans silver parquet, re-aggregates. Wasteful.

**Solution:** In-memory cache keyed by `(queryPlan, spark_version, schema_version)`. Configurable TTL. Invalidate on schema change. Optional disk spill for large results.

**Effort:** 2-3 person-days
**Impact:** High for hot dashboards — sub-second response for repeated queries.

**Files to change:**
- New: `src/main/scala/io/semanticdf/QueryCache.scala`
- `src/main/scala/io/semanticdf/SemanticTable.scala` — wrap `.toDataFrame(spark)` in cache lookup

**Consumer gate:** Once a consumer reports dashboard latency > 5s OR same query running > 10x/hour.

---

### 2.2 Materialized views (gold data)

**Problem:** Some queries are so hot they need pre-computed parquet tables, not on-demand compilation. Example: exec dashboard refreshed every minute, asking for daily revenue per country.

**Solution:** YAML-driven materialization spec. Pipeline reads the spec, generates the aggregated tables, writes them to Delta/parquet. Dashboard reads materialized tables directly.

```yaml
materializations:
  - name: daily_revenue_by_country
    grain: [country, order_date]
    measures: [total_revenue, order_count]
    refresh: daily
    destination: gold.daily_revenue_by_country
```

**Effort:** 1-2 weeks
**Impact:** High for hot paths.

**Files to change:**
- `src/main/scala/io/semanticdf/SemanticOp.scala` — extract materialization compilation
- `src/main/scala/io/semanticdf/Materializer.scala` — orchestrator
- YAML schema extension for `materializations:` block

**Consumer gate:** Once a consumer identifies a query that runs 1000x/day and wants sub-second latency.

---

### 2.3 Streaming source support

**Status:** ✅ **DONE** — shipped in v0.1.9.

**What shipped:**
- `SemanticStreamingTableOp` (in `SemanticOp.scala`) — the streaming counterpart to `SemanticTableOp`. The op tree walks it transparently (`resolveRootModel`, `findStream`, etc.).
- `toStreamingSemanticTable(spark.readStream...)` package-level factory. `YamlLoader.load(...)` auto-routes `df.isStreaming == true` to it.
- `SemanticTable.toStreamingQuery(spark, opts): StreamingQuery` — the streaming terminal (parallel to `toDataFrame`). Validates the op tree, builds the streaming query, returns the runner. Same op tree as batch; only the *terminal* (§4.5 in DESIGN) differs.
- `StreamingValidator` rejects unsupported patterns (limit, orderBy, groupBy+agg without window, stream-stream joins, `t.all` without window) before the query starts. Each error names the offending pattern.
- `StreamingQueryOptions` / `StreamingConfig` / `OutputSink` typed shapes (in `StreamingSupport.scala`).
- Worked streaming example: `examples/streaming-events/` mirroring the batch templates.

**What did NOT ship** (still deferred):
- Dynamic/late-arriving dimensions (e.g., `attributable join_one` whose static side changes mid-stream). Static-stream joins work (one side batch, one side streaming).
- Multi-source stream-stream joins. Spark itself constrains these to append mode; we accept but don't surface the constraint explicitly.
- Tighter MCIP surfacing of streaming — MCP/CLI tools focus on semantic logic; lifecycle (start/stop) is the operator's program.

**Outcome:** the streaming terminal is available for evaluation in v0.1.9. Operational hardening (restart safety, checkpoint policy, SLA-grade watermarks) is the operator's responsibility, surfaced in `examples/streaming-events/README.md`'s *Going to production* section.

---

### 2.4 Model versioning + lineage

**Status:** 🟡 **PARTIAL** — model-level versioning + lifecycle shipped; lineage track deferred.

**Shipped (model-level versioning + lifecycle):**
- `SemanticTable.version: Int` field (defaults to `0` = unversioned) and `.version(v: Int)` setter.
- `version:` block in YAML models (e.g. `flights: { version: 1 }`).
- Version propagates through `where` / `having` / `groupBy().aggregate()` / `withTransforms`-on-join.
- `YamlLoader` parses and propagates the YAML version.
- `SemanticTable.status: ModelStatus` (`Draft` / `Published` / `Deprecated`) with `.status(s)` setter.
- `status:` block in YAML models; defaults to `Published` for back-compat.
- Status propagates through every fluent op (`withDimensions` / `withMeasures` / `withRowFilter` / `orderBy` / `limit` / `hint` / `withTransforms` / `groupBy().aggregate()`).
- Lifecycle surfaces in MCP `describe_model` (`data.status`) and the manifest artifact (`model.status`).
- The library is permissive — it never fails on a version mismatch or on a `Deprecated` status; consumers (MCP server, OKF generator, agent framework) read these fields and apply their own policy.

**Problem (the still-open part):** when a YAML model changes, downstream consumers (dashboards, APIs, other models) break silently. No way to know "who uses total_revenue?"

**Solution (lineage track):** Track every loaded model version (git SHA + content hash). On load, write a row to `_semanticdf_lineage` with the model name, version, and the fields it exposes. When a model is reloaded, diff the fields and emit a "fields added/removed" event.

**Effort:** ~1 week for lineage track (versioning itself is done)
**Impact:** Medium — needed for governance but not blocking for first consumer.

**Files to change (lineage only):**
- New: `src/main/scala/io/semanticdf/ModelRegistry.scala`
- `src/main/scala/io/semanticdf/YamlLoader.scala` — emit lineage events
- New: `_semanticdf_lineage` parquet/Delta table

**Consumer gate:** Once a consumer reports "dashboard broke and we don't know why" or "we need to audit who uses what metric."

---

### 2.5 Query audit log

**Problem:** No record of who ran what query when, against which models, how long it took, how much data it scanned. Required for compliance and chargeback.

**Solution:** Every `toDataFrame(spark)` call emits an event to a configurable sink (parquet, Delta, Kafka, stdout):

```json
{
  "timestamp": "2024-03-15T14:23:01Z",
  "user": "alice",
  "model": "orders",
  "operation": "groupBy(carrier).aggregate(total_revenue)",
  "duration_ms": 1234,
  "rows_in": 1000000,
  "rows_out": 4,
  "bytes_scanned": 52428800
}
```

**Effort:** 1 week
**Impact:** Medium-High — needed for compliance.

**Files to change:**
- New: `src/main/scala/io/semanticdf/AuditLogger.scala`
- `src/main/scala/io/semanticdf/SemanticTable.scala` — emit events from `.toDataFrame(spark)`

**Consumer gate:** Once a consumer asks for compliance reporting or chargeback.

---

## Tier 3: Nice-to-have — ship after consumer validates

### 3.1 REST API / server mode

Spin up semanticdf as an HTTP service. Accept SQL-like queries (`POST /query {model: "orders", groupBy: [...], aggregate: [...]}`), return JSON. Any BI tool can hit it.

**Effort:** 1-2 weeks
**Impact:** Removes Spark dependency from BI consumers.

**Consumer gate:** Once a non-Spark consumer wants to query (e.g., a Python notebook or external dashboard).

---

### 3.2 CLI tools ✅ (v0.1.0)

**Shipped:** `docsgen`, `introspect`, `okfgen` — all under `io.semanticdf.tools.Main`, all invokable via `mvn exec:java -Dexec.args="<tool> --path <dir> --out <dest>"`. See the README's CLI Tools section for runnable examples.

**What's gated now** (the actual open piece): a SQL-like terminal query — e.g. `semanticdf query models/ "SELECT carrier, total_revenue FROM orders GROUP BY carrier"` — wrapping the Scala DSL for ad-hoc exploration. Different from the current CLIs (which work on YAML files) because it would compile and execute an arbitrary semantic-table query against the live catalog.

**Effort:** 3-4 person-days
**Impact:** Devs love this for debugging.

**Consumer gate:** Once we have ≥3 devs asking "how do I run this from the terminal?" (the gate is now narrower — the CLIs themselves already ship.)

---

### 3.3 Data quality checks in YAML

```yaml
tests:
  - name: total_revenue_positive
    expr: "total_revenue >= 0"
  - name: order_count_matches_detail
    expr: "order_count == count(order_id)"
```

Pipeline runs these after each materialization, fails the pipeline on violation.

**Effort:** 1 week
**Impact:** Catch data issues before they reach dashboards.

**Consumer gate:** Once a consumer reports "we shipped wrong numbers" or "data quality is bad."

---

### 3.4 VSCode / IntelliJ YAML schema plugin

JSON Schema for the YAML format. Autocomplete, validation, hover docs.

**Effort:** 1 week (mostly writing the JSON Schema)
**Impact:** Lower friction for YAML authors.

**Consumer gate:** Once multiple teams are authoring YAML in different repos.

---

### 3.5 dbt integration

Read dbt models as semantic tables. Build semanticdf on top of dbt's curated layer.

**Effort:** 2 weeks
**Impact:** Tap into the existing dbt ecosystem. ~30% of BI teams already use dbt.

**Consumer gate:** Once a consumer says "we already have dbt models, can we use those?"

---

### 3.6 Multi-language SDK

Python (PySpark) wrapper. Java bindings. R wrapper.

**Effort:** 2-3 weeks total
**Impact:** Largest user base expansion.

**Consumer gate:** Once multiple teams in different languages want to use semanticdf.

---

## Tier 4: Enterprise features — only with real demand

### 4.1 Row-level security / column masking

```yaml
security:
  row_filter: "region = current_user_region()"
  column_mask: { pii_field: "sha256(value)" }
```

**Effort:** 2-3 weeks
**Consumer gate:** Multi-tenant deployment with PII concerns.

### 4.2 Query cost estimation

Before executing, estimate scanned bytes from Spark plan statistics. Warn if > 1 TB scanned. Reject if > 10 TB.

**Effort:** 1 week
**Consumer gate:** Production deployment where runaway queries are a cost concern.

### 4.3 Web UI for browsing models

Browse, search, query models in a browser. Mini Cube/Preset.

**Effort:** 3-4 weeks
**Consumer gate:** Non-engineers want to explore without Spark access.

---

## What NOT to build (yet)

These are tempting but premature without a real consumer.

| Idea | Why skip |
|---|---|
| **Custom web UI** | BI tools exist. Build the API, not the UI. |
| **GraphQL endpoint** | REST/SQL is enough. Don't invent a query language. |
| **ML model serving** | Out of scope. semanticdf is about metrics, not predictions. |
| **Multi-cloud deployment** | Single-cloud works fine. Add complexity when needed. |
| **Custom UDFs in YAML** | Push back to Scala DSL. Keeps YAML simple. |
| **Time travel queries** | Built into Delta/Iceberg — don't reinvent. |
| **Schema evolution in YAML** | Breaking change to v1. Defer to v2. |

---

## Proposed order — pending consumer feedback

If we want a rough sequence to discuss:

**Phase 1 (Tier 1):** Lazy measure eval, introspect tool, error messages, docs gen, semantic EXPLAIN. ~2 weeks total.

**Phase 2 (Tier 2, gated on consumer signals):**
- Query cache — when consumer reports latency
- Materialized views — when consumer identifies hot query
- Query audit log — when consumer asks for compliance

**Phase 3 (Tier 3+):** All gated on multiple consumers + real demand.

**Skip:** Tier 4 features until a paying enterprise customer asks.

---

## Open questions for review

1. **Is "consumer signal" the right gate?** Or should we ship more aggressively to "see what sticks"? (Lean: stay conservative — over-investing features that don't ship is worse than missing a feature that nobody wanted anyway.)

2. **What about the existing first-consumer-plan.md?** Should this document replace it or complement it? My take: complement — that one is about finding a consumer; this one is about what to build once we have one.

3. **Should we publish a public roadmap?** Some projects do (Cube.dev, Materialize). Pro: signals commitment. Con: sets expectations we can't meet without consumer revenue.

4. **Should introspect become a paid feature?** Probably not. It drives adoption, which drives downstream value. But worth revisiting if there's a clear monetization path.

5. **What's the right pace?** Two features per month? One feature per week? I don't have a strong opinion yet — let's see how the next few weeks go.

---

## Decision log

- **2024-XX:** This document created. T1 features identified as universal wins. T2-T4 features gated on consumer feedback.
- **2026-07:** v0.1.1 shipped (typed withMeasures + SortKey.asc/desc overloads; ExpressionValidator for dims/transforms/measures/filters; CalcExpr.validateReferences for calculated_measures). The YAML load-time validation pass is now complete — every `expr` field in the YAML model schema has consistent fail-fast validation with explicit, documented visibility rules. The library is at 294 tests on both Spark 3.5.8 and 4.1.1; MCP server at 35 tests.
- **2026-07:** v0.1.2 shipped the `LazyTransformsOp` refactor (lazy compile contract for `withTransforms` on join models — no more `SparkSession.active` side effect).
- **2026-07:** v0.1.3 shipped (`#54` Jackson Scala module registered for case-class JSON; `#55` shared `SparkFixture` across MCP specs; `#56` `OrderByParser` accepts Scala `Map`; `#57` `examples/cli-consumer/` standalone CLI for the REST API; `#58` `Dimension`/`Measure` carry `exprString` so `describe_model` surfaces real expressions; `#59` CLI README "What building this surfaced" section closed).
- **2026-07:** Post-v0.1.3: PR `#61` (wiring `# WARN:` lines through `Introspect.handle()`; `field_inventory.skipped` agrees with `warnings.length`). PR `#62` (clean entity placeholder names in `Introspector.toJoinYaml` — bare `id` → `id_model:`, `_uuid`/`_code`/`_key` suffixes strip cleanly). Library at 329 tests; MCP at 72 tests. PR `#63` (docs refresh — version/test-count references + retire limitations fixed earlier). PR `#64` (`ResultDecoder.derive[T]` Scala 2 macro for case classes with primitive fields; closes the typeclass derivation deferral noted in `docs/phase-E-plan.md` §E1). Library at 335 tests; MCP at 72 tests; 407 total. PR `#65` (README + DESIGN + phase-E-plan + feature-roadmap docs surface `ResultDecoder.derive[T]`). PR `#66` (README intro rewrite — status banner replaced with educational intro). PR `#67` (T1 — new `docs/GLOSSARY.md` + `docs/DOCS_MAP.md` + README section reorder). PR `#68` (T2 — new `examples/README.md` central index with reader journeys). PR `#69` (T3-A — new `docs/guide.md` narrative walkthrough companion to DESIGN.md). PR `#70` (T3-B — expand `docs/guide.md` with worked examples per section + trim README Capabilities by relocating tutorial-length subsections to `guide.md`).
