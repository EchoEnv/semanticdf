# Release notes

## v0.1.17 — dbt manifest reader

A dbt `manifest.json` reader that turns dbt's manifest into a `Map[String, SemanticTable]`. Closes the gap for dbt users who don't want to hand-author a second YAML.

```scala
// Phase 1: read the manifest. Pure, no Spark needed.
val project = DbtManifestReader.read(Paths.get("target/manifest.json"))

// Phase 2: bind to a Spark session.
val tables: Map[String, SemanticTable] =
  DbtManifestReader.toSemanticTables(project, spark, sourceTable =>
    spark.read.format("parquet").load(s"/data/$sourceTable"))
```

### Wire convention

A column is a **dimension** by default. To mark a column as a **measure**, the user adds to their dbt `schema.yml`:

```yaml
columns:
  - name: total_revenue
    meta:
      kind: measure
      expr: "sum(amount)"
```

The reader checks for `meta.kind == "measure"` AND a non-empty `meta.expr`. Anything else stays a dimension — no `kind: dimension` marker (dimensions are the default).

### What's new

- `DbtManifestReader.scala` — ~290 LOC. Two-phase API: `read(manifestPath)` / `read(manifest: Map)` for parse-only; `toSemanticTables(project, spark, resolve)` for Spark binding.
- Source-table resolution: `<database>.<schema>.<alias>` / `<schema>.<alias>` / `<alias>`. Caller controls how to interpret the string.
- 13 tests in `DbtManifestReaderSpec` covering manifest parsing, column partition, source-table formatting, end-to-end Spark binding, and error paths.
- `examples/dbt-reader/` — runnable demo: hand-crafted `manifest.json` + CSVs + `Main.scala`.
- `docs/design/dbt-manifest-reader.md` — design notes (problem, convention, what's NOT in v1).

### What's NOT in v1 (deliberate, scope-limited)

- **Joins.** dbt doesn't record join keys in the manifest. v1 emits the model graph without joins; users add them via the existing `join_one` / `join_many` API.
- **Sources / metrics / streaming.** Sources are preserved in `DbtProject.sources` for v2; metrics in `rawNodes`; streaming isn't a dbt concept.

### Files

- `src/main/scala/io/semanticdf/DbtManifestReader.scala` (new)
- `src/test/scala/io/semanticdf/DbtManifestReaderSpec.scala` (new)
- `src/test/resources/dbt-fixtures/minimal-manifest.json` (new)
- `examples/dbt-reader/` (new example: `models/manifest.json`, `models/orders.csv`, `models/customers.csv`, `src/main/scala/com/example/dbtreader/Main.scala`, `pom.xml`, `README.md`)
- `docs/design/dbt-manifest-reader.md` (new design doc)
- `examples/README.md` (new row in the examples index)

## v0.1.16 — structured predicate on the MCP wire

The MCP `query` and `explain` tools now accept an optional `ast_where` (and `ast_having`) field with a structured predicate shape. Mirrors the library's `PredicateAst` ops (`eq` / `neq` / `lt` / `lte` / `gt` / `gte` / `and` / `or`). The flat `where` / `having` shape is unchanged.

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

If both `where` and `ast_where` are present, the server AND-combines them. Either can be omitted.

### What's new

- `AstPredicates.scala` — ~80 LOC parser for the structured AST shape. Detects nodes vs values by the presence of an `op` key.
- `QueryRequest` DTO — new `ast_where` and `ast_having` fields (both `Option[Any]`).
- `Query.mergedWhere` / `mergedHaving` — combine the two predicate sources. Exposed as `private[handlers]` for direct unit testing.
- `queryToolSchema` — two new properties (`ast_where`, `ast_having`) of type `object`.
- 16 new tests in `AstPredicatesSpec` (every op + every error path).
- 7 new tests in `QuerySpec` (round-trip JSON, end-to-end runs, merge logic).

### What's NOT new

- No new ops. The AST op set is the closed subset the library's `PredicateAst` actually produces. For richer predicates (`in`, `not_in`, `is_null`), use the flat `where` shape.
- No JSON Schema validation of nested `op`/`left`/`right` types. The SDK validates that `ast_where` is a JSON object; deeper shape is checked at parse time (rejected as `INVALID_PREDICATE` / `UNSUPPORTED_OP`).

### Files

- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/AstPredicates.scala` (new)
- `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/Query.scala` (DTO + handler + schema)
- `semanticdf-mcp/src/test/scala/io/semanticdf/mcp/handlers/AstPredicatesSpec.scala` (new)
- `docs/agents/mcp-contract.md` (new §"Alternative: ast_where / ast_having", updated status to v4, updated error-codes table)

## v0.1.15 — SQL-mode CLI

A **first-touch friction** release. Ad-hoc exploration of a YAML model no longer requires writing Scala. The `query` subcommand parses a SQL string and maps it to the existing `SemanticTable.query()` API.

```bash
mvn exec:java \
  -Dexec.args="query --models examples/starter/models/ \
  --sql 'SELECT carrier, total_passengers FROM flights \
  GROUP BY carrier ORDER BY total_passengers DESC LIMIT 10'"
```

### What's new

- `SqlCli.scala` — ~340 LOC tokenizer + recursive-descent parser. No external SQL dependency.
- `query` subcommand in `Main.scala` — `--models <dir-or-file> --sql '<sql>'`.
- Supports `SELECT ... FROM ... WHERE ... AND/OR ... ORDER BY ... ASC/DESC LIMIT n`. `GROUP BY` accepted (and ignored — the model decides grouping from the SELECT items).
- Aliases (`SELECT carrier AS c`), `*` (all dims then all measures), `WHERE` with `AND`/`OR` and parentheses, string literals with SQL `''` escapes.
- Classification: a name that matches a model `measures` key → measure, otherwise → dimension. Unknown field names give a Clear error listing both known dims and known measures.
- 20 new tests (16 unit + 4 end-to-end with a real Spark session). 566/566 total.

### What's NOT new

- No JDBC / ODBC surface. This is a batch CLI, not a server.
- No subqueries, no JOINs, no CTEs. The grammar is intentionally narrow: one model, one SELECT, one WHERE.
- No timezone-aware casting of string literals. Numbers are `Long` if integer, `Double` if fractional. Strings are `String`.

### Files

- `src/main/scala/io/semanticdf/tools/SqlCli.scala`
- `src/main/scala/io/semanticdf/tools/Main.scala` (one new subcommand)
- `src/test/scala/io/semanticdf/tools/SqlCliSpec.scala`
- `src/test/scala/io/semanticdf/tools/SqlCliEndToEndSpec.scala`
- `src/test/resources/sql-cli-fixtures/flights.yml`
- `docs/runtime-quickstart.md` (usage example)
- `docs/feature-roadmap.md` (1.6 SHIPPED entry)

## v0.1.14 — asymmetric join keys

A **asymmetric-key** release. `SemanticTable.join_one` / `join_many` / YamlLoader `joins:` now accept different column names on each side of the join (e.g. `flights.carrier` joined to `carriers.code`). The wire format and runtime already supported the asymmetric shape; only the entry-point guards were blocking the case. No breaking change — existing symmetric joins work unchanged.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.14
io.semanticdf:semanticdf-mcp_2.13:0.1.14
com.example:semanticdf-cli_2.13:0.1.14
```

Test count: 544 library + 90 MCP + 18 CLI (652 total), green on Spark 3.5.8 and 4.1.1.

### Library — features

- **`SemanticTable.join_one` / `join_many` accept asymmetric keys** — the typed entries (`join_on(leftKey -> rightKey)`) were already asymmetric-safe; the lambda overloads (`(l, r) => l("x") === r("y")`) now round-trip the asymmetric pair through `SemanticJoinOp.leftKeys` / `rightKeys` correctly.
- **`YamlLoader` accepts asymmetric `left_on` / `right_on`** — `joins: { carriers: { left_on: carrier, right_on: code } }` now parses and executes end-to-end. Previously rejected with a "left_on == right_on" guard.
- **`compileEquiJoin` trusts the constructor's captured keys** — no longer re-probes the lambda at compile time. The probe at construction (via `extractJoinKeys` + AST walker) is the single source of truth for both `leftKeys` and `rightKeys`.

### Performance

Strictly **less work** than v0.1.13:

- Eliminated one re-probe per `toDataFrame` call (the lambda no longer runs against the joined DataFrames at compile time just to discover keys already known from construction).
- The probe at construction runs once and its result is reused across all `toDataFrame` invocations on the same `SemanticTable`.

### Anti-scope (preserved as honest caveats)

- `onExprString` is still emitted as the legacy fallback for non-equi / OR / subquery predicates that don't fit the AST.
- `preAggregateAtGrain` (Many-cardinality fan-out prevention) probes each dimension against the side's DataFrame; user-supplied lambda dims that bypass the scope (`_ => col("x")`) are not filtered by name. Use scope-respecting dims (`t => t("x")`) for fan-out pre-aggregation to work correctly with cross-side dims.

## v0.1.13 — structured predicate AST for joined-manifest

A **predicate-AST** release. The `joined-models-manifest` recipe's last narrow caveat is closed: the joined wire shape now carries predicates as a structured AST (`model.join.predicate_ast`) alongside the legacy opaque `onExprString` SQL form. Tools get a typed view of the join condition; the reader's reconstructed `on` lambda uses the AST when present (zero overhead for the equi case, where the keys lattice already captures the structure).

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.13
io.semanticdf:semanticdf-mcp_2.13:0.1.13
com.example:semanticdf-cli_2.13:0.1.13
```

Test count: 538 library + 90 MCP + 18 CLI (646 total), green on Spark 3.5.8 and 4.1.1.

### Library — features

- **`PredicateAst` data model** — `Op` (sealed trait + 8 case objects for `eq` / `neq` / range / `and` / `or`), `Operand` (sealed trait + `ColumnRef`), `Predicate` (recursive AND/OR composition). `Predicate.toColumn(leftSide, rightSide)` builds a Spark `Column` from the AST, cached per `(leftSide, rightSide)` pair (typically 1 entry per join).
- **`PredicateAstWalker`** — cross-version reflection walker that turns a Catalyst `Expression` (Spark 3.x) or a `ColumnNode` (Spark 4.x) into a `PredicateAst.Predicate`. Handles `EqualTo` / `LessThan` / etc. via class-name fallback AND `UnresolvedFunction("=")` via `functionName`. Handles both `Seq[_]` and `java.util.List[_]` for the children collection.
- **`SemanticJoinOp.predicateAst: Option[PredicateAst.Predicate]`** — populated eagerly at construction when the keys lattice alone doesn't capture the structure (non-equi / OR / compound). Zero overhead for the canonical equi-join case.
- **`SemanticManifest.toJoinedJson` emits `predicate_ast`** when present; **`fromJoinedJson` prefers the AST** for rebuilding `on` (when prefixes aren't in play) and falls back to `onExprString` for legacy wire shapes.
- **`PredicateAstJson` helpers** — Jackson serialise/deserialise the AST.

### Wire format

The new `predicate_ast` field sits on the join block alongside `leftKeys` / `rightKeys` / `onExprString`:

```json
{
  "op": "lt",
  "left":  { "side": "left",  "col": "date" },
  "right": { "side": "right", "col": "valid_to" }
}
```

Compound predicates (AND / OR) recurse on `left` / `right`. See `examples/joined-manifest/` for an end-to-end demo (equi + non-equi + prefixed in one run).

### Anti-scope (preserved as honest caveats)

- `onExprString` is still emitted (legacy fallback). Older readers that don't know about `predicate_ast` continue to work via the SQL form.
- The structured AST covers the operations the library actually produces in practice (`eq` / `neq` / range / `and` / `or`). Anything more complex (subqueries, UDFs, etc.) falls through to `onExprString`.

### Cross-version

`ColumnSql.expressionOf` now returns `AnyRef` (was `Expression`). On Spark 4.x, when the underlying `ColumnNode` doesn't expose `.expression()` (e.g. for `UnresolvedFunction`), the node itself is returned — `PredicateAstWalker` handles both `Expression` and `ColumnNode` trees via the same reflection-driven shape match.

## v0.1.12 — joined-manifest caveats closed (Path C)

A **joined-manifest-completion** release. The `joined-models-manifest` recipe's last two BLOCK caveats are now closed: the wire shape carries `model.extra_dimensions[]` / `model.extra_measures[]` (caveat §1.2 — alias-prefixed dims round-trip) and the `join` block's `leftPrefix` / `rightPrefix` (caveat §1.3). After this release, the recipe is **ACCEPTED**; the only remaining narrow caveat is non-equi / OR predicates, which fall back to the captured `onExprString` SQL form.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.12
io.semanticdf:semanticdf-mcp_2.13:0.1.12
com.example:semanticdf-cli_2.13:0.1.12
```

Test count: 519 library + 90 MCP + 18 CLI (627 total), green on Spark 3.5.8 and 4.1.1.

### Library — features (Path C)

- **`SemanticManifest.toJoinedJson` emits alias-prefixed dims/measures** —
  the joined wire shape now carries `model.extra_dimensions[]` and
  `model.extra_measures[]` blocks for the alias-prefixed dimensions and
  measures that YamlLoader adds at runtime (e.g. `carriers.name`).
  Omitted when empty (canonical post-v0.1.11 producer case).
- **`SemanticManifest.fromJoinedJson` reconstructs them** — wraps the
  base join in a `SemanticTransformsOp` carrying the alias-prefixed
  dims/measures, matching the runtime's exact wiring. The round-trip is
  functional for the typical aliased-join case.
- **`SemanticJoinOp.leftPrefix` / `rightPrefix`** — new optional fields
  on the case class. The wire shape emits them in the `join` block
  (omitted when empty); the reader's reconstructed `on` lambda applies
  them so the predicate reads `l("<leftPrefix>k1") === r("<rightPrefix>k1")`
  when set, with bare column names when empty.
- **`JoinedManifestMeta` gains 4 fields** — `leftPrefix: String`,
  `rightPrefix: String`, `extraDimensions: Int`, `extraMeasures: Int`.
  All default to empty / 0 for legacy manifests.

### Anti-scope (preserved as honest caveats)

- Non-equi / OR predicates — fall back to the captured `onExprString`
  SQL form, which is functional for the wire-round-trip case but
  consumers that need the full predicate semantics should re-load from
  YAML.

See [`RELEASE.md`](https://github.com/EchoEnv/semanticdf/blob/v0.1.12/RELEASE.md)
for the full changelog.

## v0.1.11 — manifest keys, joined-manifest wire shape, recipe denoise

A **joined-manifest + manifest-keys + denoise** release. The library closes both BLOCKed recipes from the v0.1.11 review cycle (`manifest-transforms` and `joined-models-manifest`): `SemanticManifest` now round-trips joined models with embedded per-side single-table manifests, the join key is recovered from the wire shape (typed `join_on` entry / multi-key AND / SQL-form `onExprString` fallback), and `Model.status` carries five new identity fields (`id`, `manifestVersion`, `$schema`, `namespace`, `metadata`) through every surface. The `Transform.exprString` field round-trips through the manifest. Two new typed entry points (`join_on`, `join_many_on`) carry the join key as the source of truth at construction; the legacy lambda overload is preserved for back-compat via a probe that decomposes the AST. Cross-version: the AST probe walks `Column.expr` on Spark 3.5.x and `ColumnNode` on Spark 4.1.x via reflection.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.11
io.semanticdf:semanticdf-mcp_2.13:0.1.11
com.example:semanticdf-cli_2.13:0.1.11
```

Test count: 513 library + 90 MCP + 18 CLI (621 total), green on Spark 3.5.8 and 4.1.1.

### Library — features

- **Manifest identity + governance fields** — `SemanticManifest.Identity`
  case class + `toJson(model, identity, prettyPrint)` overload. Five new
  optional top-level fields on the single-table wire shape: `id` (reverse-DNS
  FQN, **required at write time** via `--id` flag), `manifestVersion`
  (semver, defaults to `0.1.0`), `$schema` (URL pointing at the
  `schemas/manifest.schema.json` reference), `namespace` (defaults to
  `default`), `metadata` (free-form `Map[String, String]`). `parseMeta` version
  gate relaxed to a `v0.1.*` prefix match so old manifests continue to parse
  after the schemaVersion string bumps.
- **`tools.Main manifest` gains `--id` (required), `--namespace`,
  `--metadata-K V`** — the CLI now writes the manifest with the new identity
  fields populated. **The `joined-manifest` envelope is no longer BLOCKed**:
  `toJoinedJson` (new) emits `kind: "semanticdf-joined-manifest"` with two
  embedded per-side single-table manifests, a `join` block (cardinality +
  keys), per-side dimension/measure counts, and a SQL-form `onExprString`
  fallback for non-equi predicates. `fromJoinedJson` (new) reconstructs a
  `SemanticTable` rooted at `SemanticJoinOp` with a **functional** `on`
  lambda built from the wire keys (single-column: `l(k)=r(k)`; multi-column:
  `AND over pairs`). `parseJoinedMeta` extracts the joined header without
  loading Spark.
- **Manifest `Transform[]` round-trip** — `Transform.exprString: Option[String]`
  field carries the source SQL through the wire shape. `SemanticManifest`
  emits a `transforms[]` block; `fromJson` reconstructs a
  `SemanticTransformsOp`. The `<lambda>` sentinel path still throws a loud
  runtime error on first query.
- **Manifest `CalcMeasure` round-trip** — `readMeasure` dispatches on the
  manifest's `kind` field: base measures use `F.expr` directly; calc measures
  use `CalcExpr` to walk the calc DSL and substitute `scope(name)` for each
  measure reference, so the post-aggregation `MeasureScope` resolves
  sibling-measure columns correctly.
- **Typed `join_on(other, keys)` entry** — the "core-correct and
  best-practice" way to build a join. The key names are the source of
  truth at construction; the `on` lambda is synthesised internally. Two
  overloads: `join_on(other, (String, String))` for single-key and
  `join_on(other, Seq[String], Seq[String])` for multi-key (positional
  pairing). The corresponding `join_many_on(...)` is the fan-out variant.
- **Legacy `(JoinSide, JoinSide) => Column` lambda still works** — a
  construction-time probe decomposes the AST to recover keys. Capture maps
  tag column names with side prefixes so the walker can distinguish sides
  even after bytecode resolution. The `onExprString` field carries the SQL
  form for predicates the probe can't factor (OR, non-equi, mixed).
- **Cross-version Spark compatibility** — Spark 4.1.x removed `Column.expr`
  in favor of `ColumnNode` (UnresolvedAttribute / UnresolvedFunction). A
  small `ColumnSql` reflection helper abstracts both, and the AST walker
  recognises both `Expression` (Spark 3) and `ColumnNode` (Spark 4) node
  types via `children()` + `sql()` + `functionName()` — same source compiles
  on both. The `JoinKeyProbe` capture-tag trick (`__left__id` /
  `__right__id`) survives the version jump.
- **Tolerant backward compat** — the version-gate in `parseMeta` is
  relaxed to a `v0.1.*` prefix match so v0.1.9 and v0.1.10 manifests still
  parse unchanged. The kind discriminator now accepts both
  `semanticdf-model-manifest` and `semanticdf-joined-manifest`. A new
  `CLI` subcommand `validate-joined-manifest` reads joined manifests
  without loading Spark.

### MCP server

No functional change; the joined-manifest wire shape is library-only.

### CLI

- **`manifest` subcommand** — new flags: `--id` (required, reverse-DNS
  FQN), `--namespace` (default `default`), `--metadata-K V` (repeated
  inline, no separate config file). The `validate-joined-manifest`
  subcommand prints the joined header (kind, cardinality, per-side counts,
  identity, BLOCK warning).
- **No break** for existing manifest subcommand flags.

### Docs

- **Recipe docs denoised** — `docs/design/joined-models-manifest.md` is
  now `SHIPPED cleanly`; `manifest-transforms.md` and
  `manifest-identity-bump.md` removed their "implementation landed in PR
  #NNN" suffixes; `REVIEW-FEEDBACK.md` got a Resolution Status section
  tying BLOCKs to ship-PRs.
- **`docs/manifests-and-joins.md`** — full educational walkthrough of the
  joined-manifest wire shape with the new `toJoinedJson` /
  `fromJoinedJson` API path. The §5 "real path" section uses the canonical
  library primitives; §5.5 (renamed from §5) covers the legacy
  hand-rolled alternative for pre-0.1.11 consumers.

### Examples

- **`manifest-load/`** — refreshed to demonstrate the new
  `SemanticManifest.fromJson` reading a v0.1.11-format manifest end-to-end.
- **`manifest-transforms-load/`** — new worked example showing the
  `transforms[]` round-trip and the `<lambda>` sentinel path.
- **`joined-manifest/`** — new worked example showing the canonical
  `toJoinedJson` / `fromJoinedJson` path. Uses starter's flights +
  carriers models, runs a programmatic join via `join_on`, emits the
  joined manifest, parses the header via `parseJoinedMeta`, and
  round-trips via `fromJoinedJson`.
- **`joined-manifest-split/`** — historical / reference example (the
  legacy hand-rolled envelope path). Remains valid documentation for
  consumers pinned to pre-0.1.11 versions; the README banner now
  redirects to the canonical `joined-manifest/` example.
- All 14 example pom.xml files bumped to 0.1.11. `joined-manifest` and
  `joined-manifest-split` both compile and run end-to-end against
  v0.1.11-SNAPSHOT (verified: `mvn scala:run` → "demo complete" on
  both).

### Anti-scope (preserved as honest caveats)

- Alias-prefixed dim names from the joined runtime (e.g. `carriers.name`
  in the YAML's `joins:` aliasing) don't flow through the joined wire
  shape. The merged-model state has the un-prefixed names; consumers
  needing the full alias surface re-load from YAML or call
  `joined.explainSemantic` for the runtime-resolved names.
- `leftPrefix` / `rightPrefix` on the `join` block (recipe §3) are not
  implemented. `SemanticJoinOp` doesn't carry them today; the recipe
  acknowledges this as a future revision item.

## v0.1.10 — manifest artifact, lifecycle, denoise

A **manifest + lifecycle + docs** release. The library gains a portable JSON-manifest format for shipping a model's static definition independently of YAML, a first-class `Model.status` lifecycle field (Draft / Published / Deprecated), MCP and CLI surfaces that surface lifecycle warnings on every successful envelope, and a denoised docs surface that reflects the current state of every shipped feature. Lifecycle enforcement (warnings vs refusal) is consumer-side, by design.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.10
io.semanticdf:semanticdf-mcp_2.13:0.1.10
com.example:semanticdf-cli_2.13:0.1.10
```

Test count: 442 library + 90 MCP + 18 CLI (550 total), green on Spark 3.5.8 and 4.1.1.

### Library — features

- **`SemanticManifest` artifact** — `toJson` / `fromJson` / `parseMeta`.
  Single-table models serialize to a self-contained JSON file carrying
  the model identity (name, version, status, description, sourceTable),
  every dimension, every measure (with `kind: "base" | "calc"` and the
  calc-measure dependency closure), pre-aggregate filters, and a digest
  header. Single-table is the only supported shape — joined models throw
  at `toJson` time (per the recipe §10 anti-scope). Streaming models
  record `isStreaming: true` in the digest; `fromJson` produces a
  `SemanticStreamingTableOp` for streaming sources.
- **Base + calc measures round-trip the persisted `expr` string.**
  `readMeasure` dispatches on the manifest's `kind` field: base measures
  use `F.expr` directly; calc measures use `CalcExpr` to walk the calc
  DSL and substitute `scope(name)` for each measure reference, so the
  post-aggregation `MeasureScope` resolves sibling-measure columns
  correctly. A bare lambda with no `exprString` hint records the
  `<lambda>` sentinel and throws a loud runtime error on first query.
- **`Model.status` lifecycle field** — sealed trait with `Draft`,
  `Published`, `Deprecated` cases. Round-trips through the YAML
  `status:` block (defaults to `Published` for back-compat with v0.1.x
  models). Propagates through every fluent op (`withDimensions`,
  `withMeasures`, `withRowFilter`, `orderBy`, `limit`, `hint`,
  `withTransforms`, `groupBy().aggregate()`). Surfaces in the manifest
  artifact (`model.status`) and in the MCP `describe_model` response
  (`data.status`).
- **Auto-derived time dimensions** — `Dimension.time("ts", ...,
  derive = Seq("year", "month", "day"))` materializes sibling dims
  using Spark date-part functions on the source column. The YAML
  equivalent is `derived_dimensions: [year, month, day]`.
- **Implicit `SparkSession` pass** — `toDataFrame` and `execute` now
  accept an implicit `SparkSession`, matching the Scala DSL ergonomics.
  Backward-compatible: existing explicit-`spark` call sites still work.

### MCP server

- **Lifecycle warnings on every successful envelope** —
  `SemanticManifest` and `toJson` / `fromJson` carry a `warnings:
  List[String]` field. `ManifestMeta.status == "deprecated"` or
  `"draft"` populates the warnings array. Agents see the warning in
  every tool response; downstream consumers can route on it (e.g.
  refuse to query a `deprecated` model).

### CLI

- **Lifecycle warnings on stderr, not stdout** — `sdf describe
  <model>` and `sdf list` print `WARN: model 'X' is deprecated` to
  stderr when the model is `Deprecated` or `Draft`. JSON output
  (`--json`) is unaffected; warnings stay out of the stdout payload.
- **Status column in `sdf list`** — the table gains a `STATUS` column.

### CLI consumer

- **End-to-end integration tests** — `examples/cli-consumer/` now ships
  18 integration tests that exercise every command against an in-process
  `com.sun.net.httpserver.HttpServer`. The tests catch wire-format
  regressions and the `WARN:` plumbing; they run on every PR.
- **Transport failures return exit 3 (don't kill the JVM)** — `sys.exit`
  inside `Client.send` was replaced with a `TransportFailure` exception
  that `Main.run` catches. The CLI's behavior is unchanged for human
  users (same exit code, same stderr message); tests can now exercise
  the transport-error path without killing the test JVM.

### Examples

- **`manifest-load/`** — new worked example for the manifest's
  runtime half. Reads a pre-built streaming-manifest artifact
  (`manifests/usage.json`) via `SemanticManifest.fromJson`, runs
  queries against a streaming source, and surfaces lifecycle warnings.
  Companion to the [manifest-artifact recipe](docs/design/manifest-artifact.md).
- **`streaming-manifest-load/`** — the streaming analog. Reads
  `events.json`, builds a `StreamingConfig`, runs a streaming query for
  12 seconds, prints progress. Demonstrates the streaming manifest
  read path; the manifest carries the model, the operator carries the
  runtime config.
- All 9 example templates updated to the `implicit val spark` form
  and to declare `status: published` explicitly.

### Docs

- **`docs/known-limitations.md` is current** — the summary table
  reflects the v0.1.10 state (auto-derived time dims are shipped;
  streaming lifecycle is documented; the denoise pass removed
  stale version refs and "not implemented" claims that are now
  implemented).
- **`docs/agents/mcp-contract.md` documents the lifecycle warnings**
  added to the contract.
- **3 new design recipes** (`docs/design/joined-models-manifest.md`,
  `docs/design/manifest-transforms.md`, `docs/design/streaming-manifest.md`)
  document the remaining manifest gaps with concrete DRAFT designs.
- **User-facing surface denoised** — PR-level provenance noise removed
  from user-facing docs (the policy: docs describe the WHAT, not the
  WHEN; `RELEASE.md` and `feature-roadmap.md` are the canonical home
  for changelog / historical refs).

### Compatibility

No breaking changes. All 14 PRs since v0.1.9 are additive. The
manifest's new field is optional. `Model.status` defaults to
`Published`. The streaming manifest read path is new (it was a
no-op before). The CLI's lifecycle-warnings surfacing is new.
`tools.Main manifest` is unchanged (it doesn't generate streaming
artifacts — that's an inline script in the new example's README).

### Anti-scope (carried forward, unchanged)

- **Joined models in the manifest** — still throws at `toJson` per
  recipe §10. A `joined-models-manifest.md` recipe was drafted with
  a separate `kind: "semanticdf-joined-manifest"` proposed; needs a
  library change to `SemanticJoinOp` before the implementation lands.
  Deferred to v0.2.
- **`transforms:` block in the manifest** — recipe drafted
  (`manifest-transforms.md`); needs `Transform.exprString` added to
  `Model.scala` first. Deferred.
- **Streaming joined models** — separate recipe if there's demand.
- **N-way joins, non-equi joins, schema negotiation** — out of scope
  for the manifest format. Joined models work via the YAML loader.

## v0.1.9 — Structured Streaming terminal end-to-end

A **streaming + doc + ergonomic** release. The library gains a first-class Structured Streaming terminal that shares the op tree, builders, typed DSL, validator, and implicit-SparkSession ergonomics with the batch terminal. Lifecycle (when to start, how long to run, when to stop) stays with the operator's program — by design, not by accident.

Library, MCP server, and CLI consumer are at

```
io.semanticdf:semanticdf_2.13:0.1.9
io.semanticdf:semanticdf-mcp_2.13:0.1.9
com.example:semanticdf-cli_2.13:0.1.9
```

Test count: 404 library + 72 MCP, green on Spark 3.5.8 and 4.1.1.

### Library — features

- **Streaming terminal** (`SemanticTable.toStreamingQuery(spark, opts)`).
  The streaming counterpart to `toDataFrame`. Same DSL builders
  (`groupBy` / `where` / `join_one` / `groupByDimensions` /
  `aggregateMeasures` / typed `Predicate` factories) work on
  streaming-rooted models. `StreamingValidator` rejects the patterns
  the streaming engine can't handle (limit, orderBy, stream-stream
  joins, groupBy+aggregate without a window, `t.all(...)` without a
  window) before the query starts, naming the offending pattern.
- **`SemanticStreamingTableOp`** — the streaming counterpart to
  `SemanticTableOp`. The op tree walks it transparently; the
  streaming terminal + the YAML loader + the typed DSL all
  converge on the same op node shape.
