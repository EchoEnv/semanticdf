# Release notes — v0.1

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
