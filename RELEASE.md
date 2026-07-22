# Release notes

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
