# Apache Ossie adapter

**Status:** ACCEPTED — `io.semanticdf.adapters.SemanticMetadataAdapter`
+ `DbtAdapter` + `OssieReader` shipped in the post-v0.1.16 release.

## Problem

The semantic layer ecosystem has multiple incompatible metadata
formats:

- dbt's `manifest.json` (v12+)
- Apache Ossie (formerly "Open Semantic Interchange") — a YAML
  format with 10+ vendor converters already shipped
- Cube.js (its own DSL)
- LookML
- Internal formats at every data company

Every new format = every tool that consumes a semantic layer has to
write a new adapter. **The library shouldn't have to pick a winner**;
it should support a unified entry point that works with whichever
metadata source the user already has.

## Design

A typeclass that unifies the two-phase API (parse + bind) and
lets users call the same `loadSemanticTables(...)` regardless of
format:

```scala
trait SemanticMetadataAdapter[Source, Project] {
  def parse(source: Source): Seq[Project]
  def toSemanticTables(
    projects: Seq[Project],
    spark:    SparkSession,
    resolve:  String => DataFrame,
  ): Map[String, SemanticTable]
}

object SemanticMetadataAdapter {
  def loadSemanticTables[S, P](
    source:  S,
    spark:   SparkSession,
    resolve: String => DataFrame,
  )(implicit adapter: SemanticMetadataAdapter[S, P]): Map[String, SemanticTable] =
    adapter.toSemanticTables(adapter.parse(source), spark, resolve)
}
```

Each format is one `object` that extends the trait. The
`DbtAdapter` wraps the existing `DbtManifestReader`; the
`OssieReader` is a new reader for the Apache Ossie YAML format.

### Why a trait, not parallel objects

Two formats share the same two-phase API. With a trait, callers
write a single entry point; future formats plug in as new
`SemanticMetadataAdapter` instances and inherit the entry point
for free. The cost is ~30 LOC for the trait itself; the win is
"any future format works the same way."

### Why a wrapper for dbt, not a refactor

`DbtManifestReader` already has a public surface (`read`,
`toSemanticTables`, `DbtProject`, `DbtModel`, etc.). Replacing
it with a trait-based redesign would be a breaking change.
The `DbtAdapter` is a thin wrapper — same parsing logic, same
intermediate type, same two-phase API. Dbt users keep their
existing code; the new entry point picks up dbt via the adapter.

## What ships

### `SemanticMetadataAdapter` (trait + companion)

The typeclass. Defines the two-phase API and the unified
`loadSemanticTables` entry point. ~50 LOC.

### `DbtAdapter` (wrapper)

Thin wrapper over `DbtManifestReader`. Implements the trait with
`Source = java.nio.file.Path` and `Project = DbtProject`. ~40 LOC.

### `OssieReader` (new reader)

Parses Apache Ossie YAML files (canonical `semantic_model` shape
+ legacy `ontology_mappings` shape). Implements the trait with
`Source = java.nio.file.Path` and `Project = OssieProject`. ~330 LOC.

**Ossie wire shape** (canonical, from `core-spec/spec.yaml`):

```yaml
version: "0.2.0.dev0"
semantic_model:
  - name: sales_analytics
    datasets:
      - name: orders
        source: sales.public.orders
        primary_key: [order_id]
        fields:
          - name: order_date
            expression: { dialects: [{dialect: ANSI_SQL, expression: order_date}] }
            dimension: {is_time: true}
    relationships:
      - name: orders_to_customers
        from: orders
        to: customers
        from_columns: [customer_id]
        to_columns: [id]
    metrics:
      - name: total_revenue
        expression: { dialects: [{dialect: ANSI_SQL, expression: SUM(orders.amount)}] }
```

**Mapping to semanticdf:**

| Ossie | semanticdf |
|---|---|
| `semantic_model[*].name` | one entry in `Map[String, SemanticTable]` |
| `dataset.name` | `SemanticTable.name` |
| `dataset.source` | `SemanticTable.sourceTable` (caller resolves) |
| `field` (regular) | `Dimension` (with `exprString` set) |
| `field` (`dimension.is_time: true`) | `Dimension.time` |
| `metric` | `Measure` (with `exprString` set) |
| `relationship.{from,to,from_columns,to_columns}` | `join_on(leftKey → rightKey)` |

The reader picks the `ANSI_SQL` dialect (or the first available
dialect as a fallback). Other dialects are ignored in v1.

## What's NOT in v1

- **Other dialects** (Snowflake, Databricks, BigQuery, etc.).
  Picked on read; a v2 would round-trip.
- **`ai_context` mapping** (synonyms, instructions, examples).
  Preserved on the intermediate but not used to build the
  SemanticTable.
- **The `ontology` concept declarations** (the upper half of
  the flights example). Preserved on the project for v2
  consumers; not used to build tables.
- **Composite join keys** (`from_columns` / `to_columns` arrays
  of length > 1). v1 picks the first column of each array; the
  full composite is preserved on the intermediate.
- **Metric dataset binding.** Ossie metrics are top-level
  (not inside a dataset). v1 strips the leading dataset-name
  prefix from metric expressions (`SUM(orders.amount)` →
  `SUM(amount)`) via a regex pass; this is brittle and will be
  replaced when the spec stabilizes.
- **`primary_key` / `unique_keys`.** Preserved on the dataset
  intermediate; not used to build the SemanticTable (semanticdf
  doesn't have a first-class grain concept yet).

## Perf baseline (v0.1.17)

Captured on the first run (Spark 3.5.8, JDK 17, single-machine
local mode). **Observational, not gates** — a slow CI day doesn't
block a PR, but a doubling of any of these numbers is a real
regression.

```
OssieReader.parse (small, 2KB):                          4ms
OssieReader.parse (medium, 19KB TPC-DS):                  8ms
OssieReader.parse (large, ~1MB synthetic):                52ms
OssieReader.parse (large, regex pass over 200 metrics):  26ms
```

The medium-file number (8ms for a 100-field TPC-DS-shaped model)
is the realistic production shape. The large-file stress test (52ms
for 1000 fields + 200 metrics) shows the parser scales linearly,
not quadratically. If a future PR doubles the medium-file time,
that's visible in CI.

**Leak tests** (in `OssieReaderLeakSpec`) are **gates** — a failure
means a real leak:

- A dropped parse result can be GC-collected (no static retention)
- 100 parse+drop cycles don't grow the heap beyond 50MB (catches
  runaway accumulation)
- toSemanticTables also GC-reclaims (the bind step is stateless)

## Tests

10 tests in `SemanticMetadataAdapterSpec`:

- OssieReader.parse: canonical shape → 1 project per `semantic_model`
- Field types: time vs regular dimensions
- Relationships: from/to + parallel column arrays
- Metrics: expressions preserved
- toSemanticTables: end-to-end build with real Spark
- Query against an Ossie-built table: results match expectations
- loadSemanticTables: works for Ossie
- loadSemanticTables: works for dbt
- Error: missing file
- Error: file without `version` key

The dbt test reuses the existing `minimal-manifest.json` fixture
from PR #171.

## Usage

```scala
import io.semanticdf.adapters.DbtAdapter
import io.semanticdf.adapters.OssieReader
import io.semanticdf.adapters.SemanticMetadataAdapter.loadSemanticTables

// dbt
val dbtTables = loadSemanticTables(
  Paths.get("manifest.json"), spark, resolve)

// Ossie
val ossieTables = loadSemanticTables(
  Paths.get("flights.yaml"), spark, resolve)
```

The trait resolves via implicit; import the adapter object
directly (or use the explicit call form via `DbtAdapter.toSemanticTables(DbtAdapter.parse(...), ...)`).
