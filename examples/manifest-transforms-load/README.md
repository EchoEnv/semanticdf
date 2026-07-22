# manifest-transforms-load

A runnable worked example showing how to round-trip a model with
**transforms** through the `SemanticManifest` wire format.

## What this is

Build a `SemanticTable` programmatically, attach transforms via
`withTransforms(Transform(..., exprString = Some("...")))`, emit the
manifest JSON, re-load it, and run queries against the restored model.

## Run

```bash
# First install the parent semanticdf project (from the repo root).
cd ../..
mvn -o install -DskipTests
cd examples/manifest-transforms-load
mvn -o scala:run -DmainClass=com.example.manifesttransformsload.Main
```

## What it demonstrates

1. Building a `SemanticTable` with `withTransforms(...)` and
   `withMeasures(...)`.
2. The new `Transform.exprString: Option[String]` field — populated
   so the writer can serialize the source SQL string instead of
   the `<lambda>` sentinel.
3. Emitting a manifest via `SemanticManifest.toJson(model, identity)`.
4. Reading the manifest back via `SemanticManifest.fromJson(json, source)`.
5. Running queries against the restored model: a column-projection
   query (transform column `price_with_tax` is materialized) and a
   group-by + aggregate (measure `avg_price`).

## Key file

`src/main/scala/com/example/manifesttransformsload/Main.scala` — every
step has a comment explaining what it does.

## What this is NOT

It does NOT demonstrate the `tools.Main manifest` CLI — that's the
upstream side of the workflow (YAML → JSON), and is exercised in CI
via the `ManifestIdentityAuditSpec`. This demo is the downstream side
(JSON → `SemanticTable` → query).
