# OKF Mapping for semanticdf

**Status:** v0.1.7 (current) — this spec is the per-field reference for the OKF bundle shipped by `okfgen` (see [`docs/agents/maintenance.md`](maintenance.md) for the regeneration workflow). The post-v0.1.3 library fixes (PRs `#61`, `#62`) do not change OKF output.
**Audience:** anyone editing YAML model fields and wanting to know exactly how each one is rendered into OKF Markdown.

This document is the **per-field spec** for how semanticdf YAML model files become
OKF concept documents. Every YAML field listed here has exactly one OKF
destination. If a YAML field has no OKF destination, this document does not
emit it.

---

## Invariants

1. **YAML is the source of truth.** The OKF bundle is generated *from* the YAML.
   Never edited by hand. Never used as input to the engine. Drift is a bug.
2. **Sidecar layout.** Every `.yml` becomes a sibling `.md`. Same directory, same
   basename, `.md` extension. No reorg of the YAML tree.
3. **`type: SemanticTable`.** One producer-defined type, used for every concept
   document. OKF §4.1 explicitly allows producer-defined types; consumers MUST
   tolerate unknown ones, so this works even for agents that haven't seen the
   format before.
4. **`resource:` always points at the YAML source.** The schema file is the
   canonical authoritative asset; OKF is a knowledge-layer reference to it.
5. **No duplication of schema fields.** Every value in the OKF body comes from
   the YAML through `SemanticTable` or `YamlLoader`. If the implementation finds
   itself hardcoding a string, that's a bug.
6. **Generator output is byte-stable.** Same YAML → byte-identical OKF. (One
   exception: frontmatter `timestamp` follows git, not file mtime — different git
   states yield different timestamps.)

---

## Bundle layout

```
<project>/
├── models/                              # YAML source tree
│   ├── flights.yml
│   └── carriers.yml
└── agents/                              # OKF bundle (generated)
    ├── index.md                         # bundle root index
    ├── log.md                           # bundle root changelog
    ├── okf_version: "0.1"               # declared in the index frontmatter
    └── models/
        ├── index.md                     # per-directory progressive disclosure
        ├── flights.md
        └── carriers.md
```

Generated tree mirrors the YAML tree. The mapping is mechanical: for every path
`<dir>/foo.yml`, emit `<dir>/../agents/<dir>/foo.md` (relative to the bundle
root, which is the `--out` directory passed to the generator).

The `agents/` directory name is fixed for now. Override is out of scope; if a
project already has an `agents/` directory for another purpose, pick a different
output root via the generator's `--out` flag.

---

## Frontmatter mapping

| YAML source (via `SemanticTable`) | OKF frontmatter field | Required | Notes |
|---|---|---|---|
| model name (e.g. `flights`) | `title` | yes | PascalCase: `flights_carriers` → `Flights Carriers` (split on `_`, capitalize). If the model already starts with a capital, just title-case the rest. |
| `description` | `description` | optional | Direct copy. If empty, leave field out — OKF §4.1 says optional. |
| — | `type` | **yes** | Always `SemanticTable`. Producer-defined per OKF §4.1. |
| YAML file path on disk | `resource` | yes | `file://` URI relative to the project root. The YAML is the asset; OKF is the metadata envelope. |
| YAML file's last commit ISO timestamp (`git log -1 --format=%aI <file>`) | `timestamp` | optional | Falls back to file mtime if git is unavailable. Falls back to **omitted** if neither is available — the spec allows missing `timestamp`. |
| aggregation of `metadata.tags` across the model's dimensions + measures, plus `metadata.owner` if present | `tags` | optional | See "Tag derivation" below. |

### Tag derivation

```
candidate_tags =
   owner's tag (if metadata.owner present)            →   tags: ["owner:<value>"]
 + every dimension's metadata.tags[], flattened       →   tags: [<string>...]
 + every measure's metadata.tags[], flattened
 + a structural tag                                   →   tags: ["semantic-table"]
 + the model's own metadata.tags (if any)

deduped, sorted alphabetically, then YAML-serialized as a flow list.
```

**Example:** A model whose dimensions include
`metadata.tags: [airline, identifier]` and measures include
`metadata.tags: [analytics, count]` plus owner `data-platform-team` emits:

```yaml
tags: [airline, analytics, count, data-platform-team, identifier, owner:data-platform-team, semantic-table]
```

The `owner:data-platform-team` and the bare `data-platform-team` are intentional
duplicates — the bare form is the human-readable tag; the `owner:` form is the
namespace form for filtering on resource-ownership. Both are useful to
consumers in different ways.

### Worked example — `examples/starter/models/flights.yml`

Frontmatter that the F2 generator emits for `flights.yml`:

```yaml
---
type: SemanticTable
title: Flights
description: Flight facts: per-flight distance and passenger counts
resource: file://examples/starter/models/flights.yml
timestamp: <git log of flights.yml>
tags: [airline, analytics, count, data-platform-team, identifier, miles, owner:data-platform-team, percent, ratio, semantic-table, sum]
---
```

---

## Body mapping

Body sections appear in this fixed order. If a section has nothing to emit, the
heading is omitted entirely (not rendered as an empty heading).

| Section | Source | When emitted |
|---|---|---|
| `# Schema` | `SemanticTable.dimensions` + `SemanticTable.measures` (rendered as two markdown tables) | Always (a model with zero fields is unusual; emit an empty placeholder `| (none) |` if it happens) |
| `# Filters` | YAML `filters:` block (each entry has `name`, `description`, `expr`, `metadata`) | Only when `filters:` is non-empty |
| `# Joins` | `SemanticTable.joins` (list of `{name, model, type, left_on, right_on}`) | Only when `joins` is non-empty |
| `# Calculated measures` | `calculated_measures` block in YAML | Only when present |
| `# Examples` | See "Example generation" below | Always emitted (with a `(none documented)` placeholder if no examples). Always ships 3 worked examples by default. |
| `# Citations` | The YAML source itself | Always emitted, with one citation: the `resource:` URI listed again as `[1]`. |

### `# Schema` rendering

Two tables stacked, top one dimensions, bottom one measures. Columns:

```
| name | kind | expr | description | metadata |
```

Where `kind` is `dimension` or `measure`. For calc measures (in the
`calculated_measures` block) the second table gets a third sub-section
(see `# Calculated measures`).

The `metadata` column shows the metadata as `key=value` pairs joined by
semicolons. Empty metadata → `—`.

If a field's description contains a pipe character, escape it as `\|` in
markdown so the table doesn't break.

### `# Joins` rendering

A bullet list, one entry per declared join:

```markdown
- **[carriers](carriers.md)** — one, on `carrier = carrier`
```

If the join target doesn't have an OKF document yet (e.g. it's defined
elsewhere in the bundle's hierarchy), the link uses the conventional form. Per
OKF §5.3 consumers MUST tolerate broken links, so missing siblings are not an
error — a generator warning is logged, the link still emits.

### `# Filters` rendering

A single table emitted only when the model declares a `filters:` block. Each
entry has `name`, `expr` (the Spark SQL filter expression), `description`, and
`metadata`. Pre-join semantics: filters operate on this model's source table
only, never on joined-side columns. The YamlLoader enforces source-only via
`SparkFilterValidator` — a filter referencing a non-source column fails at
model-load time, not at query time.

```markdown
# Filters

Pre-join row-level predicates on this model's source table. Applied automatically before joins.

| name | expr | description | metadata |
|------|------|-------------|----------|
| require_origin_and_carrier | `origin IS NOT NULL AND carrier IS NOT NULL` | Drop rows with null origin or carrier — flagged in upstream QA rule. | owner=data-platform-team; tags=[data-quality] |
```

The `expr` cell is rendered as inline code (`\`...\``) so the spacing
characters in SQL are preserved. Pipes inside `expr` are escaped as `\\|`.

### `# Calculated measures` rendering

Same shape as the measures table, plus an inline note that the expression
references other measures. The `expr` column is the raw text (e.g.
`total_passengers / flight_count`). The `kind` column shows `calc`. The
documentation generator does not need to compute dependencies — that's the
library's job and is out of scope for OKF.

### `# Examples` rendering

Three worked query examples, in this order:

1. **Dimensions + base measures** — the simplest valid query.
   ```json
   {"model": "flights", "dimensions": ["carrier"], "measures": ["flight_count"]}
   ```
2. **Filtered query** — adds a `where` predicate.
   ```json
   {"model": "flights", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
   ```
3. **Joined query** — pulls a field across a join.
   ```json
   {"model": "flights", "dimensions": ["carriers.name"], "measures": ["total_passengers"], "limit": 10}
   ```

These are emitted from a small hardcoded template, NOT generated per-model.
Reason: the JSON shape is the MCP contract (see `mcp-contract.md`). The
template uses model names, but the predicates use semantic standard fields
(distance, passenger counts) that exist in the example fixtures, not in
real-world models. Real models get real example prompts as a future feature;
today, the three templates ship literally so every OKF doc renders cleanly.

The body says: `(run any of the above via an MCP client pointed at this
catalog)` — a forward-looking note that ties the OKF side to the runtime side.

### `# Citations` rendering

```markdown
# Citations

[1] [flights.yml](file://examples/starter/models/flights.yml) — the source schema this document references.
```

One entry only — the YAML. Future: support other models' citations, support
external blog posts / docs as second-entry citations.

---

## `# Schema` per-field table — full spec

Rendered as:

```markdown
| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| carrier | dimension | `carrier` | Airline carrier code (IATA two-letter identifier) | owner=data-platform-team; tags=airline,identifier |
| flight_date | dimension | `flight_date` | Scheduled flight date | — |
| total_passengers | measure | `sum(passengers)` | Total passengers across all flights in the group | owner=analytics-team; unit=count; aggregation=sum |
| avg_passengers | measure | `total_passengers / flight_count` | Average passengers per flight | owner=analytics-team; unit=count |
```

If a measure's `expr` cannot be stringified cleanly (e.g. it was constructed in
Scala rather than parsed from YAML string syntax), emit `<expr>` as a
placeholder string. The OKF doc is human-readable, not a re-execution artifact.

---

## Full worked output — `flights.md`

Putting it together for `examples/starter/models/flights.yml`, the F2 generator
emits exactly this:

````markdown
---
type: SemanticTable
title: Flights
description: Flight facts: per-flight distance and passenger counts
resource: file://examples/starter/models/flights.yml
timestamp: <git log of flights.yml>
tags: [airline, analytics, count, data-platform-team, identifier, miles, owner:data-platform-team, percent, ratio, semantic-table, sum]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| carrier | dimension | `carrier` | Airline carrier code (IATA two-letter identifier) | owner=data-platform-team; tags=airline,identifier |
| origin | dimension | `origin` | Origin airport code (IATA three-letter) | owner=data-platform-team; tags=airport |
| flight_date | dimension | `flight_date` | Scheduled flight date | — |
| total_passengers | measure | `sum(passengers)` | Total passengers across all flights in the group | owner=analytics-team; unit=count; aggregation=sum |
| flight_count | measure | `count(1)` | Number of flights (rows) in the group | owner=analytics-team; unit=count; aggregation=count |
| total_distance | measure | `sum(distance)` | Total distance in miles across all flights | owner=analytics-team; unit=miles; aggregation=sum |
| avg_passengers | measure | `total_passengers / flight_count` | Average passengers per flight | owner=analytics-team; unit=count |
| avg_distance | measure | `total_distance / flight_count` | Average flight distance in miles | owner=analytics-team; unit=miles |
| pct_of_total | measure | `total_passengers / all(total_passengers)` | Fraction of all passengers served by this group | owner=analytics-team; unit=ratio; format=percent |

# Filters

Pre-join row-level predicates on this model's source table. Applied automatically before joins.

| name | expr | description | metadata |
|------|------|-------------|----------|
| require_origin_and_carrier | `origin IS NOT NULL AND carrier IS NOT NULL` | Drop rows with null origin or carrier — flagged in upstream QA rule. | owner=data-platform-team; tags=[data-quality] |

# Joins

- **[carriers](carriers.md)** — one, on `carrier = carrier`

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "flights", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "flights", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "flights", "dimensions": ["carriers.name"], "measures": ["total_passengers"], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [flights.yml](file://examples/starter/models/flights.yml) — the source schema this document references.
````

---

## `index.md` generation

Per-directory `index.md`. Three sources of entries:

1. **Concept documents** at this level (`flights.yml` → `flights.md`).
2. **Subdirectories** with their `index.md` or first concept.

The body is grouped under headings. Two strategies, F2 picks the better one
based on the directory's content:

### Strategy A — alphabetic (default)

```markdown
# Models

* [Flights](flights.md) — Flight facts: per-flight distance and passenger counts
* [Carriers](carriers.md) — Airline carrier reference data (lookup)
```

### Strategy B — by owner

If the directory contains models from multiple owners (look at frontmatter
`tags: owner:...`):

```markdown
# Owner: data-platform-team

* [Flights](flights.md) — ...

# Owner: analytics-team

* [Metrics](metrics.md) — ...
```

**Strategy A is the default.** F2 emits strategy A unless model owner
`tags: owner:...` cover more than one distinct owner, in which case it
switches to B. Strategy choice is mechanical — no heuristics about
"importance" or "size".

The root `index.md` adds an `okf_version: "0.1"` frontmatter (the only place
where frontmatter is permitted in an `index.md`, per OKF §11).

---

## `log.md` generation

Per-directory changelog, derived from git history. F2 calls:

```
git log --reverse --format='%aI %s' -- <directory>/<file>
```

…per file, then merges by date heading. Newest first per OKF §7.

Format:

```markdown
# Directory Update Log

## 2026-07-13

* **Creation**: [flights.md](flights.md)
* **Creation**: [carriers.md](carriers.md)
```

If git is unavailable, emit `log.md` containing a single note:

```markdown
# Directory Update Log

> Generation: no git history available; log not produced.
```

This is honest — refusing to fabricate a log is the right move; consumers
MUST tolerate it per the spec's permissive-conformance rule.

---

## Edge cases — decisions baked in

| Case | Decision |
|---|---|
| YAML `description` is empty | Omit `description` from frontmatter; emit `# Schema` as the body |
| YAML has zero dimensions AND zero measures | Emit `# Schema` with placeholder `| (none) |` row |
| YAML has zero joins | Omit `# Joins` section entirely |
| YAML has no `calculated_measures` | Omit `# Calculated measures` section entirely |
| YAML file path has spaces or special chars | URL-encode the `resource:` URI (`%20` etc.). Don't URL-encode the file path on disk (the generator writes there directly). |
| Model name contains dots (`foo.bar`) | Replace dots with underscores in the file basename: `foo_bar.md`. Keep the title PascalCase. |
| Model name conflicts with reserved filenames (`index.md`, `log.md`) | Append `_` to the slug, emit as `index_.md` (rare; warn). |
| Duplicate metadata keys on a field | Last-write-wins is the YAML loader's existing behavior; replicate that, don't try to fix it here. |
| Metadata value contains pipe `\|` | Escape as `\\|` in the markdown table cells |
| Metadata value contains backtick `` ` `` | Wrap the cell value in double-ticks-style code: `` `value` `` → ` `` `value` `` `. Optional — only if a real one shows up. |
| Generated markdown contains a frontmatter-delimiter line (`---`) inside the body | Escape it as `&#x2014;&#x2014;&#x2014;` (HTML entities), or quote-wrap the body. Per OKF §4 only the *first* `---` after the start is the frontmatter closer. |
| `--title-case` collisions (e.g. two models named `flights` and `FLIGHTS`) | Treat as alias; emit a single OKF doc with both YAMLs as the `resource:` list (comma-separated). Today's YamlLoader disallows duplicates at parse time so this case can't actually happen — record the behavior anyway. |
| Filter `expr:` references a column not in this model's source table | Loader rejects with `SparkFilterValidator` error at model-load time (pre-join semantics). Apply cross-table predicates via query-time `.where(...)` or add the filter to the joined-side model's `filters:` block. |
| Filter `expr:` references a joined-side column (e.g. `flights.carrier` after a join) | Same as above — loader rejects. The source-only rule is the spec; it's enforced, not suggested. |
| Filter `expr:` is malformed Spark SQL | Loader rejects with `IllegalArgumentException` containing the parser error message. The agent never sees this filter. |

---

## What this doc doesn't say — and why

| Thing | Why deferred to F2+ |
|---|---|
| **Round-trip property test** | F4's responsibility, not F1's. The test will assert the mapping tables above hold; if any holds wrongly, F4 catches it. |
| **CI hook** | F5. The shell pipeline is small but it's a separate concern from the spec. |
| **CLI flag surface for `OkfGen`** | F2. We just need `--in`, `--out`, and `--no-git` flags minimally; detail when implementing. |
| **Performance / streaming for huge bundles** | Out of scope. A bundle of 1000 models will fit in memory; revisit if real consumer hits 10k+. |
| **Concurrent writes to the same output dir** | Out of scope. `OkfGen` writes atomically (write to `.tmp`, rename) but doesn't tolerate two simultaneous invocations. This is fine — CI invokes it serially. |
| **Watch mode / incremental regen** | Out of scope. Re-run the full generator. Diffable anyway. |
| **MCP `okf_markdown` field** | F7. Today's doc stops at "OKF bundle exists on disk". The contract for piping it through MCP happens after F1–F6. |

---

## Open questions worth answering before F2

1. **`description` truncation** — OKF recommends a "single sentence summarizing
   the concept". semanticdf YAMLs sometimes have multi-paragraph `description`s.
   Truncate at sentence boundary + ellipsis? Render the first sentence only?
   Or relax to first 200 chars? **Default:** first 200 chars; emit a warning if
   truncated.

2. **Field-level OKF docs?** Should each dimension / measure get its own OKF
   concept (cross-linked from the model)? The spec allows hierarchical bundles
   — `tables/flights/order_id.md` would make an IDE-style outline of the model.
   But the YAML doesn't carry descriptions richly enough today for this to be
   useful. **Default:** NO field-level OKF docs in v1. Note in a comment.

3. **Calc measures naming.** A `calculated_measures` block emits calc measures
   to the measures table. But the sample output above shows calc measures mixed
   with base measures. Is that the right call? **Default:** mixed table, with
   `kind` column = `calc`. The semantically-distinct `# Calculated measures`
   section is for when there's enough of them to deserve their own grouping;
   today that's not the case.

4. **Validation pass.** Before emitting each file, validate the bundle conforms
   to OKF §9 (parseable frontmatter, non-empty `type`). Where does the
   validator live? `OkfGen` self-validates as it goes; the round-trip test
   validates via external Python `yaml` lib as the cross-check. **Default:**
   validate-as-you-go in OkfGen; cross-validate in F4.

5. **Should the `tags` list be sorted or insertion-order?** Sorted is
   deterministic (good for byte-stable output) but loses information about
   which tag was "primary". **Default:** sorted alphabetically. **Decided.**

---

## How this doc changes

- **Before F2 lands:** review this spec with F2's implementer. Any disagreement
  is a doc edit, not a code edit.
- **After F2 lands:** changes here are doc + generated-content changes only.
  Never an excuse to special-case anything in code.
- **When the OKF spec bumps (0.1 → 0.2):** update this doc, update
  `okf_version` in the emitted root `index.md`, and re-run `OkfGen` over
  every existing bundle.
