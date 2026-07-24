# SDFAdapter

**Status:** ACCEPTED — `io.semanticdf.adapters.SDFAdapter` shipped in the post-v0.1.16 release.

## Problem

The cross-process workflow — build phase writes a manifest, query phase reads it — used `SemanticManifest.fromJson(text, df)` directly. This was a different API from dbt and Ossie (`loadSemanticTables(...)`), and the joined manifest path took TWO `DataFrame`s, not a resolve function. Cross-process consumers had to hand-wire the DataFrame binding.

## Design

```
SemanticTable.toJson(joinedModel)
        ↓
   joined-manifest.json     <-- the cross-process artifact
        ↓
SDFAdapter.parse(path)
        ↓
SDFAdapter.toSemanticTables(projects, resolve)(implicit spark)
        ↓
loadSemanticTables(path, resolve)(implicit adapter, spark)
        ↓
Map[String, SemanticTable]   <-- the same shape dbt and Ossie produce
```

### Renamed from `ManifestAdapter` to `SDFAdapter`

`ManifestAdapter` was confusing because `manifest` is overloaded in this project:
- `SemanticManifest` (the writer/reader class)
- `joined-manifest` (the recipe name)
- The CLI `manifest` subcommand
- The `manifests-and-joins.md` design doc

`SDFAdapter` matches the package abbreviation (`io.semanticdf` → `sdf`) and the project's brand (SemanticDF). The rename is a name change only — the file structure, the trait instances, and the public API are unchanged.

### Implicit `SparkSession` on `toSemanticTables`

The trait signature changed from:

```scala
def toSemanticTables(
    projects: Seq[Project],
    spark:    SparkSession,
    resolve:  String => DataFrame,
): Map[String, SemanticTable]
```

to:

```scala
def toSemanticTables(
    projects: Seq[Project],
    resolve:  String => DataFrame,
)(implicit spark: SparkSession): Map[String, SemanticTable]
```

Spark is almost always in implicit scope in user code; making it implicit removes the boilerplate of threading it through every call. The `loadSemanticTables` entry point also takes implicit spark:

```scala
def loadSemanticTables[S, P](
    source:  S,
    resolve: String => DataFrame,
)(implicit adapter: SemanticMetadataAdapter[S, P],
    spark:    SparkSession): Map[String, SemanticTable]
```

User code now reads:

```scala
implicit val spark: SparkSession = ...
import io.semanticdf.adapters.SDFAdapter._
val tables = loadSemanticTables(Paths.get("manifest.json"), resolve)
```

instead of:

```scala
val tables = loadSemanticTables(Paths.get("manifest.json"), spark, resolve)
```

## Backward compatibility

`SemanticManifest.fromJson(text, df)` and `SemanticManifest.fromJoinedJson(text, left, right)` are **preserved** — they just carry an `@deprecated` annotation pointing at the adapter pattern. They will not be removed in 0.1.x.

The adapter is a **delegation layer** — it calls the existing methods with `df` obtained from the `resolve` callback. No new parsing logic, no behavior change, no perf overhead. The two "result equivalence" tests in `SDFAdapterSpec` verify this: they build the same `SemanticTable` via both paths and assert every field matches.

## What's NOT in v1

- **Manifest versioning / migration.** Manifests at `schemaVersion = "v0.1.11-manifest"` (or earlier v0.1.x) are accepted via prefix match. A v0.2.x → v1.0 manifest migration is a separate workstream.
- **Cross-process durability.** The manifest is still in-memory once loaded. A file-backed cache or a streaming loader is a separate workstream (already covered by the v0.1.16 cache).
- **Per-source join metadata.** The joined manifest's `left` and `right` source table names are extracted by the adapter; the actual `join_on(...)` call comes from the embedded side manifests. The adapter doesn't add join metadata beyond what the side manifests already carry.

## Tests

`SDFAdapterSpec` (11 tests, including 2 result-equivalence tests):
- parse produces the right intermediate shape
- toSemanticTables builds a working SemanticTable
- **Result equivalence (single)**: adapter == direct `fromJson` for every field
- **Result equivalence (joined)**: adapter == direct `fromJoinedJson` for every field
- Both single-table and joined forms work
- Errors: missing file, missing `kind`, unknown `kind`

`SDFAdapterLeakSpec` (3 tests, gates):
- A dropped parse result can be GC-collected
- 100 parse+drop cycles don't grow the heap beyond 50MB
- toSemanticTables result is GC-collectable

`SDFAdapterPerfSpec` (3 tests, observational):
- Parse single manifest: 0ms median
- Parse joined manifest: 0ms median
- **Overhead vs direct read: 0ms (asserted <5ms)** — the adapter is zero-cost

## Usage

```scala
import io.semanticdf.adapters.SDFAdapter
import io.semanticdf.adapters.SemanticMetadataAdapter.loadSemanticTables

implicit val spark: SparkSession = ...

// Implicit: SDFAdapter via the `_` import
val tables = loadSemanticTables(
  Paths.get("target/joined-manifest.json"),
  source => spark.read.parquet(s"/data/$source.parquet")
)
```

Or explicit:

```scala
val projects = SDFAdapter.parse(Paths.get("manifest.json"))
val tables = SDFAdapter.toSemanticTables(projects, resolve)
```
