# semanticdf-trino — Phase 2 engine adapter for Trino

This module is the **Trino engine adapter** for the semanticdf library. It is
part of **Phase 2** of the
[multi-engine design](../docs/design/multi-engine-design.md). The design
extracts a portable core (`semanticdf-core`) and surrounds it with engine
adapters — this is the **first engine adapter** for a non-Spark engine.

## Status — Phase 2 setup only

This PR establishes the **project structure** for the Trino adapter:

- `pom.xml` declaring the dependency graph: depends on `:semanticdf-core:`
  (engine-portable types) and the Trino JDBC driver.
- `src/main/scala/io/semanticdf/trino/TrinoEngine.scala` — a structural
  placeholder that imports from `core.predicate` (engine-portable) and
  declares the production API surface.
- A roadmap for the actual SQL lowering + cluster integration work.

**The actual SQL lowering, source resolution, result decoding, and
cluster integration are DEFERRED** — they land in follow-up PRs once the
`Engine` trait (the Phase 2 contract) is defined in `core.engine`.

## Why a placeholder now

The multi-engine design's **Phase 1 decision gate** is the
**Trino decision gate**: a real Trino cluster running with the same data
as the Spark tests, validating that the core ADT is sufficient to express
all the queries the Spark adapter can express today.

Setting up the project structure is the prerequisite for any actual POC
work. Without this skeleton, the first Trino implementation PR would
have to also create the module + pom + initial class — a noisier diff
that's harder to review.

This PR keeps the Trino skeleton as small as possible (one class, one
pom, one README) so the actual Phase 2 work can be reviewed in focused,
narrow PRs.

## Roadmap — Phase 2 follow-up PRs

| # | Work | Estimated LoC |
|---|---|---:|
| 1 | `Engine[R]` trait + `EngineError` ADT + `Capability` ADT in `core.engine/` (per design doc §4) | ~300 |
| 2 | SQL lowering — `CorePredicate` → Trino SQL string | ~600 |
| 3 | Source resolution — `SourceRef.ByName/ByPath/ByProvider` → Trino schema | ~400 |
| 4 | Result decoding — Trino `ResultSet` → portable `ResultRow` | ~300 |
| 5 | Cancellation — Trino `remoteStatement` cancellation | ~200 |
| 6 | Tests against a Docker Trino cluster (the **decision gate** itself) | ~500 |
| 7 | Documentation + runbook for cluster setup | ~200 |

**Total estimate: ~2,500 LoC** (matches the design doc's §7.2 budget for
the Trino adapter).

## Boundary contract (this PR enforces)

This module's `pom.xml` has **zero** `org.apache.spark.*` dependencies:

```bash
grep -r 'org.apache.spark' adapters/semanticdf-trino/
# (empty output)
```

The Trino adapter consumes the engine-portable `core.predicate.Predicate`
(never the Spark-bearing `io.semanticdf.predicate.Predicate`). This
enforces the multi-engine boundary at the **build level**: if a future
contributor accidentally adds a Spark import, the Trino artifact would
carry a transitive Spark dependency — which defeats the purpose of having
a Spark-free engine adapter.

## Library version

0.2.4 — same as `semanticdf-core` and `semanticdf-spark`. No bump in
this PR (no user-facing change).

## Why Scala 2.13

The Trino JDBC driver is Java-only; the Scala binding uses Scala 2.13 to
match the rest of the project (per `scala.version=2.13.18` in the parent
pom). Cross-compilation is not needed.

## See also

- `docs/design/multi-engine-design.md` — the full design (especially
  §4 "Adapter contract", §6 "Model.of and validation", §7.2 "Phase 2 —
  Trino and Presto")
- `semanticdf-core/` — the engine-portable ADTs this adapter consumes
- `adapters/semanticdf-spark/` — the existing Spark adapter (reference
  for what the Trino adapter needs to provide)