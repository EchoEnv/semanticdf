# dbt manifest reader

**Status:** ACCEPTED — `DbtManifestReader` shipped in v0.1.16.

## Problem

dbt users already maintain a manifest for their warehouse. They don't
want to hand-author a second YAML to expose the same models to a
semantic layer. Two sources of truth, twice the maintenance, twice
the drift.

The dbt `manifest.json` (the file `dbt parse` produces) is the most
detailed, machine-readable description of a dbt project that exists.
It carries the `schema.yml` column descriptions, types, tags, and
free-form `meta` blocks for every model. The reader turns it into a
`Map[String, SemanticTable]`.

## Wire convention

A column is a **dimension** by default. To mark a column as a
**measure**, the user adds to their dbt `schema.yml`:

```yaml
models:
  - name: orders
    columns:
      - name: total_revenue
        meta:
          kind: measure
          expr: "sum(amount)"
```

The reader checks for `meta.kind == "measure"` AND a non-empty
`meta.expr`; both must be present. Anything else stays a dimension —
there's no `kind: dimension` marker (dimensions are the default, so
the annotation would be redundant).

For dimensions, the column name is the dim's `expr` (a direct column
reference). For measures, `meta.expr` is the SQL aggregation
expression (e.g. `sum(amount)`, `count(1)`, `avg(distance)`).

## Two-phase API

```scala
// Phase 1: read the manifest. Pure, no Spark needed.
val project = DbtManifestReader.read(Paths.get("target/manifest.json"))

// Phase 2: bind to a Spark session. The resolver is the caller's
// contract for how to load a source table.
val tables: Map[String, SemanticTable] =
  DbtManifestReader.toSemanticTables(project, spark, sourceTable => {
    spark.read.format("parquet").load(s"/data/$sourceTable")
  })
```

The split lets callers:

- Validate the manifest (count models, list sources) before
  allocating a SparkSession — useful in CI and in MCP servers where
  the SparkSession is lazy-initialised.
- Control how source tables resolve. dbt can target a warehouse
  (`spark.table(...)`), local parquet (`spark.read.parquet(...)`), or
  a CSV — the reader doesn't impose a policy.
- Cache the parsed `DbtProject` and re-bind with different
  `DataFrame` resolvers (e.g. for testing or for swapping dev /
  prod datasets).

## Source-table resolution

dbt writes `database` / `schema` / `alias` as separate fields on
every model. The reader assembles a fully-qualified name:

| `database` | `schema` | `alias` | `sourceTable` |
|---|---|---|---|
| `analytics` | `main` | `orders` | `analytics.main.orders` |
| _absent_ | `main` | `orders` | `main.orders` |
| _absent_ | _absent_ | `orders` | `orders` |

The caller decides how to interpret that string. For warehouse
projects, `spark.table(sourceTable)` is the common case.

## What's NOT in v1

- **Joins.** dbt doesn't record join keys in the manifest. The
  `parent_map` / `child_map` give model dependencies but not the
  SQL structure. v1 emits the model graph without joins; users
  add them via the existing `join_one` / `join_many` API. A v2
  reader would consume a separate `joins.yml` annotation file or
  introspect the `compiled_code` for `ref()`-call patterns.
- **Sources.** dbt sources (raw warehouse tables) are preserved in
  `DbtProject.sources` for downstream readers but are not turned
  into `SemanticTable`s. v1's contract is "models only."
- **Metrics.** dbt's metric nodes (the `metrics` top-level key) are
  preserved in `rawNodes` for v2 but not parsed. The semantic
  metric concept overlaps with `Measure`; a future reader could
  project them.
- **Streaming.** dbt models are batch-only. The reader produces
  batch `SemanticTable`s.

## Example

See `examples/dbt-reader/` for a runnable end-to-end: a hand-crafted
`manifest.json` plus a `Main.scala` that reads it, binds source
tables from CSVs, and runs two queries.

## Tests

13 tests in `DbtManifestReaderSpec`:

- Manifest parsing (top-level + per-model + per-source).
- Column partition (default = dimension, `kind=measure`+`expr` =
  measure, partial markers stay a dimension).
- Source-table formatting (3 cases: db.schema.table, schema.table,
  bare alias).
- `toSemanticTables` end-to-end with a real Spark session.
- In-memory Map input (no file IO).
- Error paths (missing file, unresolvable source table).
