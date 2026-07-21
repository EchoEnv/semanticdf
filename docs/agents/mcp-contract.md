# MCP Server Contract — semanticdf

**Status:** v3 — current contract. All five tools (`list_models`, `describe_model`, `query`, `explain`, `introspect`) shipped in v0.1.4 with 72 MCP tests. Resolves the three v1 open questions using library accessors shipped in PR #6 (joins / measureKind / sourceTable), PR #2 (filters), PR #3 (version). Adds `okf_markdown` field + join-prefix one-liner rule.
**Audience:** the LLM agent (Claude, Cursor, etc.), the MCP server implementation, and reviewers.

This document is the **single source of truth** for what an MCP server exposing
`semanticdf` looks like. Every tool listed here MUST wrap exactly one existing
public method on `SemanticTable`, `YamlLoader`, or `Introspector`. If a verb is
exposed here, it exists in the library. If it exists in the library and is
missing here, that's a gap to close.

---

## Decisions baked in (defaults — flag any you want changed)

| Decision | Default | Why |
|---|---|---|
| MCP SDK | Official `io.modelcontextprotocol:mcp` Java SDK | Std + HTTP transports, prompts, resources all built-in. Hand-rolled stdio is ~80 lines but reinvents the wire protocol. |
| Transport v1 | **stdio only** | Works with Claude Desktop / Cursor / Continue with zero infra. HTTP+SSE addable as v2. |
| Server module location | New sibling dir `semanticdf-mcp/` | Keeps the library jar pure: `semanticdf` is the library; `semanticdf-mcp` is one consumer of it. |
| Result format | JSON records (rows = arrays of values, columns = name+type) | Universally consumable; LLM-friendly. Arrow can replace it later for >10k rows. |
| Source-of-truth | Library — every tool is a thin adapter to one method | Mirrors karpathy-guidelines #2 (simplicity first). The MCP layer never re-implements query semantics. |
| Lifecycle | Single SparkSession, lazy init on first `query` call, kept warm | SparkSession init is 5–10s. Pooling/per-request is premature. |
| Data binding | Server reads a YAML config file mapping table names → source paths | The agent doesn't have a way to push DataFrames to the server. Config-file-as-input mirrors how library examples wire `loadDir`. |
| Multi-tenant / RLS | None in v1 | Defer until a second real user. |
| Prompts / Resources in v1 | **Defer** | Both are easy server-side, but they bloat the contract. Add as v2 once we know what agents need. |
| `okf_markdown` in `describe_model` | **Inline string**, generated at server startup via `OkfGen` | One tool call, no separate file lookup. `include_okf: false` lets compact-mode clients omit it. |
| Join one-liner format | `<left>.<key> → <right>.<key> (<cardinality>)` (one key) or `<left>(<k1, k2>) → <right>(<k1, k2>) (<cardinality>)` (composite) | Single sentence per join. Agents don't need to recompose `left_on`/`right_on`/`type` mentally. |

---

## Server lifecycle

```
# Start (stdio transport, single user)
# All three arguments are required.
semanticdf-mcp --models ./models/ --data ./data-config.yaml --okf-bundle /tmp/okf/
```

Where `--data` is a YAML file like:

```yaml
data:
  flights_csv:
    path: /data/raw/flights
    format: parquet          # parquet | csv | json | delta (delta = explicit DataSource lookup)
    readOptions:
      # format-specific — see Introspector.fromFile for the supported map
  carriers_csv:
    path: /data/raw/carriers.csv
    format: csv
```

The server:
1. Parses models YAML → `Map[String, SemanticTable]` via `YamlLoader.loadDir(...)`.
2. Pre-resolves the `data:` block → `Map[String, DataFrame]` once (at startup).
3. Holds both maps in memory; each tool call looks up by name.
4. Lazy-creates a single `SparkSession` on the first `query` call.
5. Routes all logs to **stderr** (stdout reserved for JSON-RPC — MCP hard requirement).

---

## Result envelope

Every tool returns a JSON object shaped:

```json
{
  "status": "ok",
  "data": <tool-specific payload>,
  "warnings": ["..."],
  "meta": {
    "elapsed_ms": 123,
    "rows_in_result": 50,
    "truncated": false,
    "rows_scanned": 12000,
    "model": "flights"
  }
}
```

### Per-model `filters` field

`describe_model` and (transitively) `list_models` carry a top-level `filters` array
on every model. Entries come from the YAML `filters:` block — pre-join row-level
predicates declared on the source table, applied automatically before any join.

```yaml
flights:
  filters:
    require_origin_and_carrier:
      expr: "origin IS NOT NULL AND carrier IS NOT NULL"
      description: "Drop rows with null origin or carrier."
      metadata:
        owner: data-platform-team
        tags: [data-quality]
```

```json
// describe_model response
{
  "model": "flights",
  "filters": [
    {
      "name": "require_origin_and_carrier",
      "description": "Drop rows with null origin or carrier.",
      "expr": "origin IS NOT NULL AND carrier IS NOT NULL",
      "metadata": {"owner": "data-platform-team", "tags": "data-quality"}
    }
  ],
  ...
}
```

**Contract:**

- **Source-of-truth:** the YAML `filters:` block. The library stores filters in the
  op tree as `SemanticRowFilterOp` nodes; `SemanticTable.filters` walks the tree to
  expose them. MCP `describe_model` reads from the same accessor — one definition,
  three lenses (YAML / Scala DSL / MCP).
- **Pre-join semantics:** each `expr:` operates on THIS model's source table only.
  The YamlLoader validates every `expr:` field via the YAML load-time
  validation pass (`ExpressionValidator` for dims/transforms/measures,
  `CalcExpr.validateReferences` for `calculated_measures`,
  `SparkFilterValidator` for filters) so a misconfigured expression fails
  at model-load time, not at query time. Each validator parses the
  expression and rejects references to columns/measures not visible at
  that point.
- **Always applied:** the agent doesn't pass `filters` to `query` and doesn't need
  to know they exist. They're baked into every compiled query automatically.
- **Distinct from query-time `where`:** for cross-table predicates (a filter that
  references joined-side columns), use the `query` tool's `where` parameter
  instead. Query-time `where` is composed atop any source filters.
- **OKF side:** the OKF `# Filters` section renders the same data as a markdown
  table — the agent can read it without an MCP call.

### Per-model `version` field

`describe_model` and `list_models` responses carry a top-level `version` integer
on every model. The value is whatever the YAML declared in its `version:` field,
or `0` if the model has no `version:` declared (i.e. the pre-versioning era).

```yaml
flights:
  version: 1      # → version: 1 in describe_model + OKF frontmatter
  table: ...
```

```json
// describe_model response
{
  "model": "flights",
  "version": 1,
  ...
}
```

Semantics:
- The library never fails on a version mismatch — it only stores and emits the value.
- Bumping is informational; consumers (this server, agent frameworks) decide
  what "different" means. See `SemanticTable.version` docstring for the library-side contract.
- `0` means "no version committed"; treat as "I don't know what version this is".

### Per-model `okf_markdown` field

`describe_model` carries a top-level `okf_markdown` string on every model. It is
the **OKF (Open Knowledge Format)** sidecar doc for that model — the same Markdown
that `okfgen` writes to disk under `agents/reference/<project>/<model>.md`. The
agent gets it inline; no separate file lookup needed.

```json
// describe_model response
{
  "model": "flights",
  "okf_markdown": "# flights\n\n**Flight facts:** per-flight distance and passenger counts\n\n**Source table:** `flights_csv`\n\n**Version:** 1\n\n## Dimensions (3)\n\n| name | type | description | is_entity |\n|---|---|---|---|\n| carrier | categorical | Airline carrier code (IATA two-letter) | true |\n..."
}
```

**Contract:**

- **Source-of-truth:** `OkfGen.generate(modelDir, bundleDir)` — the same tool the
  CLI exposes. The server runs it once at startup, then caches each model's
  Markdown string in memory keyed by model name. Live regeneration per request
  is not required (YAML changes are a server restart).
- **Bundle layout:** server config accepts `--models <dir>` (the YAML source
  directory) and `--okf-bundle <dir>` (where the server can write/check the
  bundle). At startup: `OkfGen.generate(<models-dir>, <bundle-dir>)` to ensure
  fresh content; then `Files.readString(<bundle-dir>/<model>.md)` into the cache.
  See [`docs/agents/okf-mapping.md`](okf-mapping.md) for the mapping rules.
- **Opt-out:** the request accepts `{"model": "flights", "include_okf": false}`
  for compact-mode callers. Default is `true` (most agents want it).
- **What's in it:** dimensions table, measures table, joins table, filter list,
  grain, version, and a one-paragraph textual summary — everything an agent
  needs to author a follow-up query without calling `list_models` +
  `describe_model` separately.
- **What it does NOT replace:** `describe_model`'s structured JSON fields
  (`dimensions`, `measures`, `joins`, `filters`). The Markdown is the **human-
  readable** mirror; the JSON is the **machine-typed** mirror. Agents building
  predicates or query parameters must read the JSON, not the Markdown.

### Join one-liner rule

Each `joins` entry in `describe_model`'s response carries a `summary` string — a
single sentence that an LLM can read without recomposing the structured fields
mentally. The format is:

```
<leftPrefix>.<leftOn> → <rightPrefix>.<rightOn> (<cardinality>)
```

- Single-key join: `flights.carrier → carriers.carrier (one)`
- Composite-key join: `orders.(order_id, line_id) → line_items.(order_id, line_id) (many)`
  — when two or more equi-join keys, wrap each side in parens and comma-separate
- Cross join: `a. → b. (cross)` — both key lists empty; omit the leading `.`
- Anonymous side (no `name: Some(...)` on `SemanticTable`): use the literal
  `left` / `right` — e.g. `left.customer_id → customers.customer_id (one)`

**Source-of-truth:** constructed by the MCP adapter from `JoinInfo` fields
(`cardinality`, `leftName`, `rightName`, `keys`) — the server does not compute it
in the library; the library exposes the structured fields and the adapter formats.

**Multi-pass:** the rule is computed per-join in the order declared in the YAML
`joins:` block. A model with three joins renders three summary lines, one per
join. The agent doesn't need to walk the op tree.

For failures:

```json
{
  "status": "error",
  "error": {
    "code": "MODEL_NOT_FOUND",
    "message": "No model named 'flightts' is loaded. Available: flights, carriers.",
    "hint": "Did you mean 'flights'?"
  }
}
```

Error codes — keep this list closed:

| Code | Triggered by |
|---|---|
| `MODEL_NOT_FOUND` | model name not in `loadDir(...)` map |
| `AMBIGUOUS_MEASURE` / `AMBIGUOUS_DIMENSION` | name matches multiple fields across joined models (e.g. `flights.carrier` vs `carriers.carrier`) |
| `UNKNOWN_FIELD` | measure/dimension name not on the resolved model |
| `INVALID_PREDICATE` | JSON predicate shape is malformed (missing `op`, bad `type`, etc.) |
| `UNSUPPORTED_OP` | predicate op not in {`eq`,`ne`,`lt`,`le`,`gt`,`ge`,`in`,`not_in`,`is_null`,`is_not_null`} |
| `QUERY_TIMEOUT` | query exceeded `MCP_QUERY_TIMEOUT_MS` (env, default 30s) |
| `EXECUTION_ERROR` | Spark execution failure — bubbles up `e.getClass.getSimpleName` + `e.getMessage` |
| `RESULT_TOO_LARGE` | rows after `toDataFrame` exceed `MCP_MAX_ROWS` (default 10000) and request omitted `limit` |

For `UNKNOWN_FIELD`, `AMBIGUOUS_*`, the `hint` field MUST be computed via
`io.semanticdf.closestMatch` — same helper the library itself uses for
dimension/measure typo recovery. Do not invent suggestions in the MCP layer.

---

## Tools

### Tool 1: `list_models`

**Purpose:** bootstrap — agent learns what models exist before doing anything else.

**Request:**
```json
{}
```

**Response `data`:**
```json
{
  "models": [
    {"name": "flights", "description": "Flight facts: per-flight distance and passenger counts"},
    {"name": "carriers", "description": "Airline carrier reference data (lookup)"}
  ]
}
```

**Library call:** `models.keys.map { n => (n, models(n).root.name) }` plus description via `models(n).root.description`.

---

### Tool 2: `describe_model`

**Purpose:** full schema of one model — agent learns dimensions, measures, joins before crafting a query.

**Request:**
```json
{"model": "flights", "include_okf": true}
```

`include_okf` is optional (default `true`). Set to `false` for compact responses
that omit the (potentially large) `okf_markdown` field.

**Response `data`:**
```json
{
  "model": "flights",
  "version": 1,
  "description": "Flight facts: per-flight distance and passenger counts",
  "source_table": "flights_csv",
  "filters": [
    {
      "name": "require_origin_and_carrier",
      "description": "Drop rows with null origin or carrier — flagged in upstream QA rule.",
      "expr": "origin IS NOT NULL AND carrier IS NOT NULL",
      "metadata": {"owner": "data-platform-team", "tags": "data-quality"}
    }
  ],
  "dimensions": [
    {
      "name": "carrier",
      "expr": "carrier",
      "type": "categorical",
      "is_entity": true,
      "is_time_dimension": false,
      "smallest_time_grain": null,
      "description": "Airline carrier code (IATA two-letter identifier)",
      "metadata": {"owner": "data-platform-team", "tags": "airline,identifier"}
    },
    {
      "name": "flight_date",
      "expr": "flight_date",
      "type": "categorical",
      "is_entity": false,
      "is_time_dimension": true,
      "smallest_time_grain": "day",
      "description": "Scheduled flight date",
      "metadata": {}
    }
  ],
  "measures": [
    {
      "name": "total_passengers",
      "kind": "base",
      "expr": "sum(passengers)",
      "description": "Total passengers across all flights in the group",
      "metadata": {"owner": "analytics-team", "unit": "count", "aggregation": "sum"}
    },
    {
      "name": "avg_passengers",
      "kind": "calc",
      "expr": "total_passengers / flight_count",
      "description": "Average passengers per flight",
      "metadata": {"owner": "analytics-team", "unit": "count"}
    }
  ],
  "joins": [
    {
      "name": "carriers",
      "cardinality": "one",
      "left": "flights",
      "right": "carriers",
      "keys": ["carrier"],
      "summary": "flights.carrier → carriers.carrier (one)",
      "extra_dimensions": [],
      "extra_measures": []
    }
  ],
  "okf_markdown": "# flights\n\n**Flight facts:** per-flight distance and passenger counts\n\n**Source table:** `flights_csv`\n\n**Version:** 1\n\n## Dimensions (3)\n\n| name | type | description | is_entity |\n|---|---|---|---|\n| carrier | categorical | Airline carrier code (IATA two-letter) | true |\n…"
}
```

**Library call (v2):**
- `models(model).description` and `models(model).sourceTable` (set by `YamlLoader`, see PR #6)
- `models(model).version` — `Int`, `0` = unversioned
- `models(model).filters` → walk `SemanticFilter.name` / `description` / `expr` / `metadata`
- `models(model).dimensions` → walk `Dimension.name`, `Dimension.expr.toString` (synthetic — `expr` is a `SemanticScope => Column`), `Dimension.isEntity`, `Dimension.isTimeDimension`, `Dimension.smallestTimeGrain`, `Dimension.description`, `Dimension.metadata`
- `models(model).measures` → same walk on `Measure`; **`kind` is read off `models(model).measureKind(name)` which returns `MeasureKind.Base | .Calc`** (PR #6, no need to re-implement the classifier in the adapter)
- `models(model).joins` → `Seq[JoinInfo]`, walked for `name`, `cardinality`, `leftName`, `rightName`, `keys`, `extraDimensions`, `extraMeasures` (PR #6, `JoinKeyProbe` captures join keys at construction time so the adapter doesn't need to compile)
- `models(model).joins.map(buildJoinSummary)` for the one-liner — see [Join one-liner rule](#join-one-liner-rule) for the format
- For `okf_markdown`: read from a server-local cache populated at startup by `OkfGen.generate(<models-dir>, <bundle-dir>)`

> **Note on `expr`:** since PR `#58` (`Dimension`/`Measure` carry
> `exprString`) the MCP layer surfaces the original expression string
> verbatim for YAML-loaded models:
> `expr = d.exprString.getOrElse(d.expr.toString)`. Programmatic
> constructions (e.g. `Dimension("foo", t => t("bar"))` without an
> `exprString` hint) still fall back to `lambda.toString`, which prints
> as opaque addresses like `io.semanticdf.YamlLoader$$$Lambda$.../1234`;
> for those, callers should populate `exprString = Some("bar")` at
> construction. In all cases, the LLM only ever needs the **name** to
> construct queries; the `expr` field is informational and shown for
> debugging.

---

### Tool 3: `query`

**Purpose:** the workhorse — run a query, return rows.

**Request:**
```json
{
  "model": "flights",
  "dimensions": ["carrier"],
  "measures": ["total_passengers", "flight_count", "avg_passengers"],
  "where": [
    {"type": "ge", "field": "distance", "value": 500},
    {"type": "in", "field": "origin", "values": ["JFK", "LAX"]}
  ],
  "having": [
    {"type": "gt", "field": "flight_count", "value": 10}
  ],
  "order_by": [{"field": "total_passengers", "direction": "desc"}],
  "limit": 20,
  "time_grain": "month",
  "time_range": ["2024-01-01", "2024-12-31"]
}
```

**Top-level fields** map 1:1 to `SemanticTable.query(...)`:

| JSON | Library arg |
|---|---|
| `model` (required) | resolved in server, picks the right `SemanticTable` |
| `dimensions` | `dimensions = Iterable[String]` |
| `measures` (required, ≥1) | `measures = Iterable[String]` |
| `where` | JSON list → `Option[List[Predicate]]` via `Predicate.And(...)` |
| `having` | same shape → same construction |
| `order_by` | `Iterable[SortKey]` — `"direction": "asc"/"desc"` → `SortKey.asc(...)`/`desc(...)` |
| `limit` | `Option[Int]` |
| `time_grain` | `Option[String]` |
| `time_range` | `Option[(String, String)]` (start, end) |

**Predicate JSON shape** maps 1:1 onto `Predicate.{Compare, In, IsNull, And, Or, Not}`:

```json
// Compare
{"type": "ge", "field": "distance", "value": 500}

// In
{"type": "in", "field": "origin", "values": ["JFK", "LAX"]}
{"type": "not_in", "field": "origin", "values": ["JFK"]}

// IsNull
{"type": "is_null", "field": "carrier"}
{"type": "is_not_null", "field": "carrier"}

// Compound
{"type": "and", "predicates": [/* predicates... */]}
{"type": "or",  "predicates": [/* ... */]}
{"type": "not", "predicate": { /* a predicate */ }}
```

Allowed `type` values for leaf predicates: `eq`, `ne`, `lt`, `le`, `gt`, `ge`,
`in`, `not_in`, `is_null`, `is_not_null`. Anything else → `INVALID_PREDICATE`.

**Construction:** `where` is a *flat list* of predicates that the server
combines with `Predicate.And(...)` before passing to `.query(where = ...)`. Same
for `having`. The agent never writes `And` wrappers manually.

```scala
// Server-side adapter (sketch — not the implementation)
val preds: List[Predicate] = req.where.map(_.map(jsonToPredicate))
val where: Option[Predicate] = if (preds.isEmpty) None else Some(Predicate.And(preds: _*))
val st = models(req.model).query(
  measures = req.measures,
  dimensions = req.dimensions,
  where = where,
  having = /* same */,
  orderBy = req.order_by.map(o => if (o.direction == "desc") SortKey.desc(o.field) else SortKey.asc(o.field)),
  limit = req.limit,
  timeGrain = req.time_grain,
  timeRange = req.time_range.map((s, e) => (s, e)),
)
val df = st.toDataFrame(spark)
```

**Response `data`:**
```json
{
  "columns": [
    {"name": "carrier", "type": "string"},
    {"name": "total_passengers", "type": "long"},
    {"name": "flight_count",      "type": "long"},
    {"name": "avg_passengers",    "type": "double"}
  ],
  "rows": [
    ["AA", 12000, 60, 200.0],
    ["UA",  9000, 45, 200.0]
  ],
  "row_count": 2
}
```

Columns are emitted by reading the resulting `DataFrame.schema`. Rows are
`df.collect().map(_.toSeq).map(_.map(toJsonValue))` — typed JSON values
(strings quoted, numbers raw, nulls `null`, timestamps as ISO-8601 strings).

**Hard cap:** server enforces `MCP_MAX_ROWS` (default 10000) on the **post-collect**
side. If exceeded and the request omitted `limit`, return `RESULT_TOO_LARGE`
with the proposed `limit` value in `error.details.suggested_limit`.

---

### Tool 4: `explain`

**Purpose:** the agent gets a plan trace before asking for the rows. Use this to
debug wrong numbers without paying the full `toDataFrame` cost on wrong queries.

**Request:** identical to `query`.

**Response `data`:**
```json
{
  "explain": "{\n  \"op_tree\": [...],\n  \"joins\": [...],\n  \"filters\": {\"pre\": [...], \"post\": [...]},\n  \"predicates\": [...],\n  \"warnings\": [...]\n}",
  "warnings": ["calc avg_passengers depends on flight_count — order is enforced"]
}
```

**Library call:** `models(req.model).query(...).explainSemantic(Some(spark))`.

The `explain` string is the library's JSON plan dump. The agent is expected to
read it. We don't second-guess the format — that's the library's job, and the
library is the single source of truth for plan structure.

---

### Tool 5: `introspect`

**Purpose:** auto-generate YAML for a DataFrame the agent hasn't modeled yet.
Lets the agent discover tables → models → queries in one session.

**Request:**
```json
{
  "table": "raw_warehouse_orders",
  "format": "parquet",
  "path": "/data/warehouse/orders",
  "model_name": "orders",
  "read_options": {"mergeSchema": "true"}
}
```

| JSON field | Library arg |
|---|---|
| `path` | `path` |
| `format` | `format` (default `parquet`) |
| `model_name` | `modelName` |
| `read_options` | `readOptions` |

**Response `data`:**
```json
{
  "yaml": "<the YAML source>",
  "field_inventory": {"dimensions": 8, "measures": 5, "skipped": 2},
  "warnings": ["field 'raw_payload' (stringType) was skipped — no obvious dimension/measure classification"]
}
```

**Library call:**
```scala
val yaml = Introspector(spark = spark).fromFile(
  spark, path = req.path, format = req.format,
  modelName = req.model_name, readOptions = req.read_options)
```

`field_inventory` is parsed from the YAML header comments; `warnings` is the
list of strings the Introspector emits when it can't classify a field.

---

## Invariants the server MUST preserve

1. **Library is the only source of truth for query semantics.** The MCP server
   never re-parses SQL, never re-walks the op tree, never invents a measure.
   Every tool's adapter is a *shallow* translation: JSON → library call →
   result → JSON.
2. **No new exceptions.** Library exceptions are surfaced verbatim with the
   same message; the server only re-classifies them into MCP error codes via
   the closed table above. No exception invented in the adapter.
3. **No mutating endpoints in v1.** All tools are read-only.
4. **Single SparkSession**, shared across calls. Never create one per request.
5. **Logs to stderr only.** stdout = JSON-RPC. (MCP hard requirement.)
6. **`closestMatch` for typos.** Server reuses `io.semanticdf.closestMatch` for
   typo suggestions. No hand-rolled Levenshtein in the adapter layer.
7. **`RESULT_TOO_LARGE` is a fast rejection.** Check the projected limit
   *before* `collect()` when feasible; worst case, `collect().length` is the
   ground truth.

---

## Out of scope for v1

| Thing | Why deferred |
|---|---|
| HTTP+SSE transport | stdio covers desktop agent use; remote/server is a deploy story, not a contract change |
| MCP prompts / resources | Easy server-side, but bloat the contract; add once we know what agents actually need |
| Arrow result format | JSON is fine under 10k rows; promote to Arrow if benchmarks demand |
| Multi-tenant / row-level security | Hard; defer until a second real user shows up |
| Streaming queries (`SemanticStreamingTableOp`, `toStreamingQuery`) | Shipped in v0.1.9. The agent surface stays the same (model-only); lifecycle is intentionally the operator's program (not an MCP tool). |
| Row mutation / writes | All v1 tools are read-only. Writes belong in a separate "admin" MCP namespace |

---

## Resolved questions (v1 → v2)

All three v1 open questions are now answered by library accessors that shipped
between v1 and v2:

1. **`measure.kind` — RESOLVED.** PR #6 added `SemanticTable.measureKind(name): MeasureKind`
   returning `Base` / `Calc`. The MCP adapter reads this directly;
   no need to re-implement a classifier. Shape unchanged from v1.
2. **`joins` access — RESOLVED.** PR #6 added `SemanticTable.joins: Seq[JoinInfo]`
   (with eager `JoinKeyProbe` capture so join keys are available at construction
   time without compiling). `JoinInfo` carries `cardinality`, `leftName`,
   `rightName`, `keys`, `extraDimensions`, `extraMeasures`. The adapter maps
   directly; `summary` is derived in the adapter per the [one-liner rule](#join-one-liner-rule).
3. **Source-table lookup — RESOLVED.** PR #6 added `SemanticTable.sourceTable: Option[String]`,
   propagated by `YamlLoader` (it passes `sourceTable = Some(<table-name>)` to
   `toSemanticTable`). The MCP adapter maps it directly to `source_table`.

---

## What's still open (v2 → server implementation)

None blocking. Two minor follow-ups tracked separately:

- **Arrow result format** — JSON is fine under 10k rows; promote to Arrow if benchmarks demand.
- **MCP prompts / resources** — both are easy server-side, bloat the contract. Add once we know what agents need.

---

## How this doc changes

- **Before code lands:** pin this contract. Read it twice. Disagree with
  anything? Change it here, not in the code.
- **After code lands:** any change to a tool is a breaking change for every
  agent already pointing at it. Document breaking changes in
  [`RELEASE.md`](../../RELEASE.md) and bump the contract version.
- **Schema versioning:** when the JSON shape changes incompatibly, copy to
  `mcp-contract-v3.md` (etc.) and start version negotiation server-side. The
  same library jar must remain source-compatible with both versions during the
  deprecation window.

---

## v1 → v2 changelog

What changed between the v1 "open questions" draft and this version. Read this
before reviewing the rest of the doc.

**Added:**
- **Per-model `okf_markdown` field** on `describe_model`'s response. Source =
  `OkfGen` output, generated at server startup and cached in memory. New
  `include_okf` request field (default `true`) for compact-mode clients.
- **Per-join `summary` field** rendered by the [one-liner rule](#join-one-liner-rule).
  Agents can read the join graph in sentences without recomposing the
  structured fields.
- **Two new decision rows** in §"Decisions baked in" for OKF inclusion and
  join one-liner format.
- **`Resolved questions` section** replacing `Open questions` — all three
  v1 questions now have library accessors (PR #6 in all three cases).

**Changed:**
- **`describe_model.joins` shape.** v1 had `name/model/type/left_on/right_on`
  (a single-key join). v2 has `name/cardinality/left/right/keys[]/summary/
  extra_dimensions[]/extra_measures[]` — `keys` is a list (handles composite
  joins), `summary` is the one-liner, `extra_*` are names added post-join.
- **`describe_model` request accepts `include_okf`** (optional, default true).
- **`describe_model` measure.kind** comes from `SemanticTable.measureKind(name)`
  (returns `Base`/`Calc`), not a hand-rolled classifier in the
  adapter. Same shape as v1 — no change in resolution, just a stable
  library accessor instead of a self-rolled classifier.

**Removed:**
- The `model/explain()` `(spark)` fabrication note for joins — no longer
  needed now that the library exposes `.joins` directly.
