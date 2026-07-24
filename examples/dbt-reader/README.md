# dbt-reader example

Demonstrates `DbtManifestReader` — the adapter that turns a dbt
`manifest.json` into a `Map[String, SemanticTable]`.

## What's in here

- `models/manifest.json` — a hand-crafted dbt manifest v12 fixture
  (mirrors what `dbt parse` would produce for a two-model project).
- `models/orders.csv`, `models/customers.csv` — backing CSVs for the
  two models.
- `src/main/scala/com/example/dbtreader/Main.scala` — the demo:
  parses the manifest, binds the source tables, and runs two queries.

## Running

From this directory:

```bash
mvn scala:run
```

Expected output: a parsed manifest header, then two query results —
`orders by total_revenue` (top 5) and `orders filtered to customer 100`.

## The convention

A column becomes a **dimension** by default. To mark it as a **measure**,
add a `meta: { kind: measure, expr: <sql> }` block in the dbt
`schema.yml`:

```yaml
columns:
  - name: total_revenue
    meta:
      kind: measure
      expr: "sum(amount)"
```

The dbt reader picks up the `kind` and `expr` keys and routes the
column into `withMeasures(...)` instead of `withDimensions(...)`.
Everything else stays a dimension — there's no `kind: dimension`
marker (dimensions are the default).
