# Feature Roadmap & Performance Plan

**Status:** Draft for review
**Last updated:** Phase E + 2 templates shipped, no consumer yet

This plan lists the features and performance improvements that would benefit semantica, organized by tier and gated on real consumer feedback. It does **not** commit to a timeline — every feature here should be re-evaluated after we have a first consumer.

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
- `src/main/scala/io/semantica/SemanticOp.scala` — `SemanticAggregateOp.compileWithBase` filters by `measureNames`
- Add a test that confirms unrequested measures aren't in the Spark plan

**Why T1:** Universal pain — every wide-model query is affected. No consumer signal needed.

**What shipped:**
- `ColumnPruningSpec.scala` — 8 regression tests that assert which aggregation functions appear in the Spark plan for various request shapes (1 measure, 2 measures, calc with transitive deps, calc + unrelated measure, empty measures, non-existent measure, result schema correctness).

---

### 1.2 Auto-generate YAML from a DataFrame

**Status:** ✅ **SHIPPED** (commit 31dd61f)

**Problem:** A new user has a parquet table and wants a semantic model. Today they must hand-write the YAML from scratch — listing every column, writing Spark SQL expressions, picking aggregation types. This is the #1 onboarding friction.

**Solution:** A CLI tool: `mvn exec:java -Dexec.mainClass=io.semantica.tools.Main -Dexec.args="introspect <path>"` reads a parquet/csv, infers:
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

**Tests:** 4 new tests in `HardeningSpec.scala` (unknown measure suggestion, close-but-wrong names, nothing-close guard, atTimeGrain dimension suggestion). Phase 1b pre-existing calc-typo test now passes. **121/121 total.**

**Effort:** 2-3 person-days
**Impact:** Medium — every error path benefits.

**Why T1:** Universal — every error message improves. Cheap.

---

### 1.4 Auto-generate HTML/Markdown documentation from YAML

**Problem:** The schema catalog (parquet) is queryable but not browsable. A new analyst doesn't know what's available unless they read every YAML file or run a Spark query.

**Solution:** A CLI tool: `semantica docs models/ [--format html|md] [--output docs/]` reads all YAML, produces a static site with:
- Index page: every model with description
- Per-model page: dimensions (with type, entity/time flags), measures (with unit, format), metadata, joins
- Per-field page: who owns it, when it was added (git blame), example queries

**Effort:** 3-4 person-days
**Impact:** Medium-High — makes the catalog actually usable by non-engineers.

**New files:**
- `src/main/scala/io/semantica/tools/DocsGen.scala`
- `src/main/resources/templates/model-page.html` (Handlebars or simple string template)
- README section showing the output

**Why T1:** Documentation compounds. Once you have the generator, you can re-run it on every model change.

---

### 1.5 EXPLAIN that shows semantic intent

**Problem:** `model.explain()` shows the op tree. `model.explain(spark)` shows Spark's physical plan. Neither explains **why** semantica routed a filter to WHERE vs HAVING, or why a join was pre-aggregated.

**Solution:** A new method `model.explainSemantic(spark)` that produces a human-readable plan:

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

**Effort:** 3-4 person-days
**Impact:** Medium — debugging slow queries becomes much easier.

**Files to change:**
- `src/main/scala/io/semantica/SemanticTable.scala` — add `explainSemantic(spark)`
- New internal: a `SemanticPlanRenderer` that walks the op tree and produces the explanation

**Why T1:** Already have the building blocks (`SemanticOp` exposes routing info, `explain()` shows the op tree). Just need to render it for humans.

---

## Tier 2: Ship when consumer needs it

These are valuable but require real consumer pain to justify.

### 2.1 Query result caching

**Problem:** BI dashboards hit the same query every refresh (1 min, 5 min, etc.). Each refresh re-scans silver parquet, re-aggregates. Wasteful.

**Solution:** In-memory cache keyed by `(queryPlan, spark_version, schema_version)`. Configurable TTL. Invalidate on schema change. Optional disk spill for large results.

**Effort:** 2-3 person-days
**Impact:** High for hot dashboards — sub-second response for repeated queries.

**Files to change:**
- New: `src/main/scala/io/semantica/QueryCache.scala`
- `src/main/scala/io/semantica/SemanticTable.scala` — wrap `.toDataFrame(spark)` in cache lookup

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
- `src/main/scala/io/semantica/SemanticOp.scala` — extract materialization compilation
- `src/main/scala/io/semantica/Materializer.scala` — orchestrator
- YAML schema extension for `materializations:` block

**Consumer gate:** Once a consumer identifies a query that runs 1000x/day and wants sub-second latency.

---

### 2.3 Streaming source support (ADR 0002)

**Problem:** The op tree is source-agnostic (DESIGN §4.4), but `SemanticTableOp` only accepts batch `DataFrame`. Streaming sources fail.

**Solution:** Add a `SemanticStreamOp` that wraps a `StreamingQuery`, produces incremental updates to a semantic table. Composite stream + batch (e.g., fact table is streaming, dimension table is batch).

**Effort:** 3-4 weeks
**Impact:** High — unlocks real-time use cases.

**Files to change:**
- `src/main/scala/io/semantica/SemanticOp.scala` — add `SemanticStreamOp`
- New: `src/main/scala/io/semantica/StreamingTerminal.scala`
- New: `SemanticTable.toStreamingQuery(spark): StreamingQuery`

**Consumer gate:** Once a consumer explicitly asks for streaming/real-time. Until then, batch is enough.

---

### 2.4 Model versioning + lineage

**Problem:** When a YAML model changes, downstream consumers (dashboards, APIs, other models) break silently. No way to know "who uses total_revenue?"

**Solution:** Track every loaded model version (git SHA + content hash). On load, write a row to `_semantica_lineage` with the model name, version, and the fields it exposes. When a model is reloaded, diff the fields and emit a "fields added/removed" event.

**Effort:** 1 week
**Impact:** Medium — needed for governance but not blocking for first consumer.

**Files to change:**
- New: `src/main/scala/io/semantica/ModelRegistry.scala`
- `src/main/scala/io/semantica/YamlLoader.scala` — emit lineage events
- New: `_semantica_lineage` parquet/Delta table

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
- New: `src/main/scala/io/semantica/AuditLogger.scala`
- `src/main/scala/io/semantica/SemanticTable.scala` — emit events from `.toDataFrame(spark)`

**Consumer gate:** Once a consumer asks for compliance reporting or chargeback.

---

## Tier 3: Nice-to-have — ship after consumer validates

### 3.1 REST API / server mode

Spin up semantica as an HTTP service. Accept SQL-like queries (`POST /query {model: "orders", groupBy: [...], aggregate: [...]}`), return JSON. Any BI tool can hit it.

**Effort:** 1-2 weeks
**Impact:** Removes Spark dependency from BI consumers.

**Consumer gate:** Once a non-Spark consumer wants to query (e.g., a Python notebook or external dashboard).

---

### 3.2 CLI tool

`semantica query models/ "SELECT carrier, total_revenue FROM orders GROUP BY carrier"`. Terminal-friendly exploration. Wraps the existing Scala API.

**Effort:** 3-4 person-days
**Impact:** Devs love this for debugging.

**Consumer gate:** Once we have ≥3 devs asking "how do I run this from the terminal?"

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

Read dbt models as semantic tables. Build semantica on top of dbt's curated layer.

**Effort:** 2 weeks
**Impact:** Tap into the existing dbt ecosystem. ~30% of BI teams already use dbt.

**Consumer gate:** Once a consumer says "we already have dbt models, can we use those?"

---

### 3.6 Multi-language SDK

Python (PySpark) wrapper. Java bindings. R wrapper.

**Effort:** 2-3 weeks total
**Impact:** Largest user base expansion.

**Consumer gate:** Once multiple teams in different languages want to use semantica.

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
| **ML model serving** | Out of scope. semantica is about metrics, not predictions. |
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
