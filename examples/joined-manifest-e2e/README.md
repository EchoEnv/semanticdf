# joined-manifest-e2e

A focused example showing the **full artifact workflow** for joined
manifests: source YAML → `toJoinedJson` → JSON artifact on disk →
`loadSemanticTables` (via the `SDFAdapter` typeclass instance) →
`SemanticTable` → analytics queries.

This is the same shape as `dbt` and `Ossie` use: one
`loadSemanticTables(source, resolve)` entry point works for all
three formats. The `SDFAdapter._` import brings the matching adapter
into implicit scope; the `resolve` function turns each `sourceTable`
string in the manifest into a `DataFrame`.

## What this is

The joined-manifest recipe (`docs/design/joined-models-manifest.md`)
demonstrates round-trip + asymmetric-key support, but no example
exercises the full **artifact-on-disk** workflow that real consumers
use in production. This example fills that gap.

Two phases, run as separate `mvn scala:run` invocations — they
represent the CI / deploy step and the runtime app:

```
   Build phase (CI)              Query phase (runtime)
   ──────────────────            ──────────────────────
   YAMLs + CSVs                   artifact JSON on disk
        │                                  │
        ▼                                  ▼
   SemanticManifest.              loadSemanticTables(...)
     toJoinedJson                   │  (via SDFAdapter)
        │                          │  resolve(source) → DataFrame
        ▼                          ▼
   target/clinical_                SemanticTable
   encounters.joined-              (restored joined model)
   manifest.json                            │
                                            ▼
                                       .query(...)
                                       (analytics)
```

## Run

```bash
cd examples/joined-manifest-e2e

# Phase 1: emit the joined manifest artifact
mvn -o scala:run -DmainClass=com.example.joinedmanifeste2e.Build

# Phase 2: load the artifact from disk and run analytics.
# Two parallel entry points — same artifact, different caller-side
# API style. Pick whichever fits your codebase.
mvn -o scala:run -DmainClass=com.example.joinedmanifeste2e.Query       # string-based .query() API
mvn -o scala:run -DmainClass=com.example.joinedmanifeste2e.TypedQuery # phantom-typed field refs
```

### String-based (`Query.scala`) vs typed-DSL (`TypedQuery.scala`)

Both load the same JSON artifact and run semantically identical queries.
They differ in how the caller addresses dims + measures:

| Style | Example |
|---|---|
| String-based (`Query.scala`) | `.query(dimensions = Seq("department"), measures = Seq("encounter_count"))` |
| Typed DSL (`TypedQuery.scala`) | `.groupByDimensions(department).aggregateMeasures(encounterCount)` |

The typed-DSL pattern (`Refs` object + phantom-typed carriers) compiles a
typo into a compile error. The string-based pattern is terser for ad-hoc
queries and tools (the `MCP` server uses it for the same reason). See
`examples/telco-analytics/` for a richer typed-DSL example with calc
measures added in Scala.

## What's interesting

- **Asymmetric key**: `encounters.primary_diagnosis` joined to
  `diagnoses.icd_code`. Different column names on each side. This is
  the v0.1.14 capability — the join engine now supports asymmetric
  keys end-to-end.
- **Artifact on disk**: the JSON file `target/clinical_encounters.
  joined-manifest.json` is the only thing that crosses the Build /
  Query boundary. The Query phase does not load any YAML.
- **Source-free inspect**: `SemanticManifest.parseJoinedMeta(json)`
  reads the header without spinning up a Spark session. Use it as
  a fast pre-flight check.
- **Self-describing**: the artifact carries `identity`, dims,
  measures, joins, predicate shape, per-side metadata. The runtime
  app only needs the artifact + the source DataFrames.

## What's NOT shown

- **3+ table joined models** — the wire format encodes joined models
  as a 2-way chain; `fromJoinedJson` takes 2 source DataFrames. To
  handle N-way, chain joins at the caller.
- **Joined-side dim queries** — `diagnoses.category` is exposed as
  an `extraDimension` on the joined side, but the public `.query()`
  API addresses base dims by bare name. The current canonical way
  to access joined-side dims is via `restored.joins.head.extraDimensions`
  (shown in Q4 of the Query phase). The joined-manifest example
  has the same limitation.
- **Streaming joined manifests** — out of scope for this example;
  the joined manifest is a batch-format artifact.

## Files

```
joined-manifest-e2e/
├── README.md
├── pom.xml
├── models/
│   ├── clinical_encounters.yml    # joined model (base + joins block)
│   └── diagnoses.yml               # per-side single-table manifest
├── data/                           # CSVs (subset of hospital/data/)
│   ├── encounters_clean.csv
│   └── diagnoses.csv
└── src/main/scala/com/example/joinedmanifeste2e/
    ├── Build.scala                 # phase 1: emit artifact
    ├── Query.scala                 # phase 2: load + queries (string-based)
    ├── TypedQuery.scala            # phase 2: load + queries (typed DSL)
    └── Logger.scala                # template-local logger
```

## See also

- `examples/joined-manifest/` — emits + round-trips a joined manifest
  in the same process; the canonical path for `toJoinedJson` /
  `fromJoinedJson`. This example extends it with the on-disk
  artifact + analytics workflow.
- `examples/manifest-load/` — the single-table equivalent of this
  example (loads a JSON manifest and runs queries against it).
- `examples/hospital/` — the source of the CSV data + per-side
  YAMLs used here.
