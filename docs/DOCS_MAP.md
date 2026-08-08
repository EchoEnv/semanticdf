# Documentation map — where to read for what

A short wayfinding guide. Pick the journey that matches what you're
trying to do.

## By journey

| If you want to... | Start here |
|---|---|
| Get a feel for SemanticDF in 5 minutes | [`README.md`](../README.md) — the new four-section intro |
| **Get semanticdf running in your own project** (paste-and-run consumer setup) | **[`docs/getting-started.md`](getting-started.md) — 5-minute Maven + SparkSession + first query** |
| Run the example models and see real output | [`examples/README.md`](../examples/README.md) — central index with a recommended order for each reader type |
| Use SemanticDF from Scala code in your own project | [`docs/getting-started.md`](getting-started.md) — paste-and-run consumer setup, then [`README.md`](../README.md) → `## Capabilities` |
| Define a model in YAML instead of Scala | [`examples/starter/`](../examples/starter/) (simplest YAML model) and [`docs/runtime-quickstart.md`](runtime-quickstart.md) |
| Tune runtime behavior — caps, caching, audit, broadcast, materialize, skew handling | [`docs/tutorial-runtime-tuning.md`](tutorial-runtime-tuning.md) — the six runtime knobs in one walk-through |
| See all six runtime knobs applied together | [`examples/runtime-tuning/`](../examples/runtime-tuning/) — customer analytics dashboard |
| See skew handling applied to a hot-key join | [`examples/skewed-join/`](../examples/skewed-join/) — 1M events, 90/10 split |
| Connect an LLM agent to SemanticDF | [`semanticdf-mcp/README.md`](../semanticdf-mcp/README.md) and [`docs/agents/mcp-contract.md`](agents/mcp-contract.md) |
| **Add a new engine / catalog adapter to the library** (write a `MyPlatform` adapter for a new SQL warehouse or REST catalog) | [`docs/agents/adding-a-new-adapter.md`](agents/adding-a-new-adapter.md) + [`templates/example-adapter/`](../templates/example-adapter/) skeleton |
| Drive the framework as a CLI client over REST | [`examples/cli-consumer/README.md`](../examples/cli-consumer/README.md) |
| Understand how a query compiles to a Spark plan | [`docs/guide.md`](../docs/guide.md) — narrative walkthrough, or [`DESIGN.md`](../DESIGN.md) §4 (architecture) for the formal version |
| Implement or extend an engine adapter (Trino, DuckDB, custom) | [`docs/design/multi-engine-design.md`](design/multi-engine-design.md) — the `Engine[R]` contract, portable IR, capability surfaces | Engine-adapter authors |
| Understand the catalog identity + CAS publication contract | [`docs/design/multi-engine-design.md`](design/multi-engine-design.md) §5.3 | Catalog-adapter authors; readers tracking model versioning |
| Run queries through the long-running platform runtime (post-crash recovery, draining, audit) | [`semanticdf-platform/README.md`](../semanticdf-platform/README.md), [`docs/design/platform-architecture.md`](design/platform-architecture.md), [`docs/design/platform-determinism-audit.md`](design/platform-determinism-audit.md) |
| Learn why we made a particular design call | [`docs/adr/`](adr/) — three ADRs, each short |
| Find an unfamiliar term | [`docs/GLOSSARY.md`](GLOSSARY.md) |
| See what's in scope and what's on the roadmap | [`docs/known-limitations.md`](known-limitations.md) (current scope + guardrails + roadmap hints) and [`docs/design/v0.3.1-feature-parity-backlog.md`](design/v0.3.1-feature-parity-backlog.md) (v0.3.0 → v0.3.1 priority list) |
| Look up a specific API method | [`README.md`](../README.md) → `## API reference` |
| See what changed in the last release | [`RELEASE.md`](../RELEASE.md) — version-by-version |
| Investigate a phantom-type compile error | the `SemanticField` scaladoc and `docs/backlog-type-safety.md` (deferred follow-ons) |
| Write or improve a Scaladoc block | [`docs/scaladoc-style.md`](scaladoc-style.md) — the bar, with rules and a pre-commit checklist |
| Write or change an error-handling / `Either` / `Option` boundary | [`docs/design/error-handling-style.md`](design/error-handling-style.md) — typed `Either` at public boundaries; `Option` for "may not exist"; `try/catch` only at IO boundaries |

## Doc roles

Each document has *one* job. We're migrating toward that structure.

| Doc | Role | Audience |
|---|---|---|
| `README.md` | Front door. Problem framing, capabilities overview, links to everything else. | Everyone |
| `docs/guide.md` | Narrative walkthrough: how a query compiles, calc measures, joins, terminals, typed layer, notebook escape hatch. Pairs with DESIGN.md the way a guidebook pairs with a map. | New readers / users |
| `DESIGN.md` | Architecture of record. Op tree, calc compilation, op-tree compilation, package layout, build & dependency strategy. | Contributors who need to understand *how* the framework works internally |
| `docs/design/multi-engine-design.md` | The engine-portable design: `Engine[R]` contract, portable IR (`RelOp`), portable result types, capability surfaces, CAS publication contract. The reference for engine-adapter authors. | Engine-adapter authors; readers following the v0.3.0 migration |
| `docs/design/v0.3.1-feature-parity-backlog.md` | The 7 gaps between the v0.3.0 portable design and full feature parity with the legacy Spark library. Prioritized roadmap: Spark-on-portable migration, `t.all`, joins, predicate unification, rollup compile, catalog adapter. | v0.3.1 contributors; readers asking "what's missing for parity?" |
| `docs/GLOSSARY.md` | Terms-of-art reference. | Everyone — especially new readers |
| `docs/getting-started.md` | The canonical consumer setup: prerequisites, Maven dep, SparkSession, sample DataFrame, first model, first query. One paste-and-run path from `mvn install` to printed output. | New users — start here |
| `docs/DOCS_MAP.md` | This document. Wayfinding. | Everyone |
| `docs/runtime-quickstart.md` | Toolchain reference: what runs on what, how to handle Java 17 + Spark, etc. | Contributors |
| `docs/tutorial-runtime-tuning.md` | Walk-through of the six runtime knobs (`withMaxRows`, `withResultCache`, `withAuditSink`, `withBroadcastJoinThreshold`, `withMaterialize`, `withSalt`). Decision tree, when-to-use matrix, real-world scenario, anti-patterns. | New users tuning a model for production |
| `docs/calc-author-guide.md` | How to define calc measures in Scala and YAML. | Calc authors |
| `docs/known-limitations.md` | Current scope, guardrails, and roadmap hints. | Readers evaluating fit |
| `docs/scaladoc-style.md` | The bar for Scaladoc in `io.semanticdf.*`. Seven rules + a pre-commit checklist + a do/don't section. The canonical example is `src/main/scala/io/semanticdf/Model.scala`. | Contributors writing or reviewing docs |
| `docs/design/error-handling-style.md` | The error-handling style: typed `Either[L, X]` at public boundaries, `Option[X]` for "may not exist", `try/catch` only at IO boundaries. Per scala-data-driven-refacer §1 + scala-chaos-testing §2. | Contributors writing or reviewing public APIs |
| `docs/adr/` | Recorded decisions. Date-stamped, terse. | Contributors |
| `docs/agents/` | LLM-agent integration — MCP contract, OKF mapping, maintenance workflow. | Agent builders |
| `docs/agents/reference/` | Per-example OKF bundles (generated by `okfgen`, checked in). | Agent readers |
| `RELEASE.md` | Changelog. Version-by-version. | Returning readers |
| `examples/README.md` | Central index for the example templates. | Anyone learning by running |
| `examples/*/README.md` | One per example template — what it demonstrates, how to run it. | Anyone learning by running |
| `semanticdf-platform/` | The standalone Restate-native platform runtime. Long-running JVM, hosting the 5 services, with post-crash query reconciliation. | Operators deploying the platform |
| `docs/design/platform-architecture.md` | Architecture of the platform runtime: state placement rule (Restate journal vs Postgres), service topology, lifecycle. | Platform contributors |
| `docs/design/platform-determinism-audit.md` | Determinism guarantees of the library when called from a Restate handler (determinism boundary + out-of-scope calls in the platform). | Platform contributors reasoning about replay safety |

## Reading order suggestions

If you have **30 minutes** and want to form a useful mental model:
1. [`README.md`](../README.md) — the four-section intro
2. **[`docs/getting-started.md`](getting-started.md) — 5-minute paste-and-run consumer setup** (run this before reading further)
3. [`docs/guide.md`](../docs/guide.md) — narrative walkthrough (companion to DESIGN.md)
4. [`examples/starter/`](../examples/starter/) — read its README, then run it (`mvn scala:run -DmainClass=com.example.starter.Main` from the example dir)

If you have **2 hours** and want to use SemanticDF:
1. The 30-minute path above
2. [`README.md` → `## Capabilities`](../README.md) — read each subsection you're likely to use
3. [`docs/calc-author-guide.md`](calc-author-guide.md) — if you write calc measures
4. [`semanticdf-mcp/README.md`](../semanticdf-mcp/README.md) + [`docs/agents/mcp-contract.md`](agents/mcp-contract.md) — if you're connecting an LLM agent

If you want to use SemanticDF in your own Scala project:
1. [`docs/getting-started.md`](getting-started.md) — the consumer setup (Maven dep + first query)
2. [`examples/README.md`](../examples/README.md) — pick the example that matches your use case
3. Then read [`docs/guide.md`](../docs/guide.md) for the conceptual walkthrough that explains *why* each piece works

If you have **half a day** and want to contribute:
1. Both paths above
2. [`DESIGN.md`](../DESIGN.md) — especially §4 (architecture) and §6 (the hard problems)
3. [`docs/adr/`](adr/) — three ADRs
4. [`docs/runtime-quickstart.md`](runtime-quickstart.md) — Java 17 + Spark quirks
