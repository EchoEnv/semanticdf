# Lineage

**Status:** DRAFT — design review.

## Problem

A semantic layer is most useful when consumers (LLM agents, BI tools,
humans) can answer two questions:

1. **Where does this metric come from?** A new agent asks "what is
   `pct_of_total`" and needs to know which base columns feed it.
2. **What will I break?** A senior engineer changes a source table
   and needs to know which downstream models will silently go wrong.

Neither question has a good answer today. `SemanticManifest` carries
the static model identity (id, namespace, status, dimensions,
measures) but not the **provenance** of each field — the base columns
each derived column reads, the upstream models a join depends on, the
calc-measure chain. The audit log tracks runtime events (which
queries touched which model) but not the static graph.

This design adds a static-analysis lineage: given a `SemanticTable`,
produce a data shape that says, for every field, which base columns
it reads and which other fields it depends on.

## Approach

A pure, versioned lineage artifact built by walking the op tree. No
runtime instrumentation. The static graph is enough for the 80% case
(answering "what is this field?" and "what depends on this model?")
and is honest about what it can't recover (lambda-only fields with
no `exprString` are opaque).

## Data model

```scala
// io.semanticdf.lineage

sealed trait LineageStatus
object LineageStatus {
  case object Complete extends LineageStatus  // every base column + dependency resolved
  case object Partial  extends LineageStatus  // some resolved, some opaque
  case object Opaque   extends LineageStatus  // no exprString; built from a Scala lambda
}

sealed trait SourceKind
object SourceKind {
  case object Batch     extends SourceKind
  case object Streaming extends SourceKind
}

final case class ColumnLineage(
  name:        String,                      // "carrier", "pct_of_total", "ts"
  kind:        ColumnKind,                  // Dimension | Measure | Transform
  baseColumns: Seq[String],                 // the actual base columns the field reads
  dependsOn:   Seq[String] = Seq.empty,     // other FIELD NAMES in the same model (for calc measures)
  exprString:  Option[String] = None,       // the SQL form, if available
  status:      LineageStatus,               // how much we could resolve
)

final case class JoinLineage(
  leftModel:   String,                      // modelId of the left side
  rightModel:  String,                      // modelId of the right side
  keys:        Seq[(String, String)],       // (leftKey, rightKey)
  cardinality: String,                      // "one" | "many" | "cross"
)

final case class ModelLineage(
  modelId:        String,                   // identity (see "Model identity" below)
  modelName:      String,
  sourceTable:    Option[String],           // None ⇒ "Unknown" sentinel
  sourceKind:     SourceKind,
  status:         ModelStatus,
  dimensions:     Seq[ColumnLineage],
  measures:       Seq[ColumnLineage],
  transforms:     Seq[ColumnLineage],
  joins:          Seq[JoinLineage],         // empty for single-table models
  upstreamModels: Seq[String],              // modelIds this model depends on via joins
)

final case class WorkspaceLineage(
  models:        Map[String, ModelLineage], // modelId → lineage
  upstreamOf:    Map[String, Set[String]],  // modelId → modelIds that depend on it
  downstreamOf:  Map[String, Set[String]],  // inverse
)
```

`WorkspaceLineage` is the whole graph. The `upstreamOf` / `downstreamOf`
indexes are pre-computed so consumers don't walk the graph on every
query.

## The entry point

```scala
object Lineage {
  def of(st: SemanticTable): ModelLineage
  def workspaceOf(models: Map[String, SemanticTable]): WorkspaceLineage
  def toJson(wl: WorkspaceLineage, prettyPrint: Boolean = true): String
  def fromJson(json: String): WorkspaceLineage
}
```

`of` and `workspaceOf` are pure functions — same input, same output.
`toJson` / `fromJson` are Jackson-based, schema-versioned (`"schema":
"semanticdf-lineage-v1"`), round-trippable.

## The analysis

The op tree is the source of truth. `Lineage.of(st)` walks it with
the existing `SemanticOpVisitor` and builds the lineage case classes
directly. No new traversal infrastructure needed.

| Op case | Lineage contribution |
|---|---|
| `SemanticTableOp(table)` | `sourceTable = <table name>`, `sourceKind = Batch` |
| `SemanticStreamingTableOp(stream)` | `sourceTable = <stream name>`, `sourceKind = Streaming` |
| `SemanticJoinOp(left, right, …)` | one `JoinLineage` per join; `upstreamModels` populated from the workspace map |
| `SemanticTransformsOp(_, transforms)` | one `ColumnLineage(kind = Transform)` per transform |
| `SemanticAggregateOp(_, _, measureNames)` | one `ColumnLineage(kind = Measure)` per measure in `measureNames` |
| `SemanticFilterOp`, `SemanticRowFilterOp`, `SemanticOrderByOp`, `SemanticLimitOp` | no lineage contribution (these are presentation/filter ops, not derivation) |

For each field with an `exprString`, `ColumnRefExtractor.extract`
parses the SQL and returns the referenced base columns. This is a
**new** extractor — distinct from the existing
`ExpressionValidator` / `CatalystColumnValidator`, which normalize
the result (lowercase, drop qualifiers). For lineage we need the
case-preserved, qualifier-preserved form, because that's what the
user wrote.

Calc-measure dependencies are detected by scanning the `exprString`
for references to other measure names (the same model's
`st.measures.keys`). If the reference is unambiguous, it's listed
in `dependsOn`. If the `exprString` is `None` (Scala-lambda-built),
the field is `Opaque` and `dependsOn = Seq.empty`.

## Data shape crossings (data-oriented view)

| # | Crossing | Shape in | Shape out | Earned sub-package? |
|---|---|---|---|---|
| 1 | `SemanticTable` → `ModelLineage` | op tree | lineage graph | **YES — `io.semanticdf.lineage`** |
| 2 | `exprString: String` → `Seq[String]` | SQL with case + qualifiers | list of column refs | NO — internal transform |
| 3 | `ModelLineage` → `String` (JSON) | case classes | Jackson text | NO — adapter in the same file |

The one crossing that earns a sub-package is #1. The `SemanticTable`
is a tree with `compile(spark)` methods; `ModelLineage` is a graph
with no execution. They are fundamentally different shapes crossing
a real boundary — the same kind that earned `io.semanticdf.result` for
`Row → T`.

## Package layout

```
io.semanticdf/
├── SemanticTable*.scala
├── SemanticOp*.scala
├── predicate/, adapters/, audit/, cache/, result/
│
├── lineage/                        ← NEW
│   ├── ModelLineage.scala          ← 5 case classes (the data model)
│   ├── Lineage.scala               ← entry point + 4 transforms
│   └── ColumnRefExtractor.scala    ← SQL → column refs
│
├── examples/, tools/
```

3 files, flat inside `lineage/`. Volume is small (~260 LOC); cohesion
is high (case classes reference each other). Nested sub-packaging
would be convention, not data.

## Model identity

`SemanticTable` has `name: Option[String]` and `sourceTable:
Option[String]`, but no canonical model ID. `ManifestMeta.id` is
optional and only populated at serialization time.

**MVP decision**: use `name` as the ID, with the same caveats the
YAML loader has today (two models with the same name collide). When
`ManifestMeta.id` is set, use it. The `name`-as-ID approach is honest
about the limitation and ships fast. A proper `id: String` field
on `SemanticTable` is a future-version concern, not an MVP concern.

## MVP limitations (honest)

1. **No canonical model ID.** Two models with the same name collide.
   Same constraint as the YAML loader. Fix is a future-version
   explicit `id` field.

2. **Scala-lambda-built fields are `Opaque`.** Fields built from raw
   Scala lambdas with no `exprString` set have `baseColumns =
   Seq.empty` and `status = Opaque`. We can't recover the source
   from a compiled lambda. By design.

3. **No runtime lineage.** The audit log already covers query-time
   lineage. This design is **static** only — what the model
   definition says, not what queries actually did.

4. **No row-level filter lineage.** `SemanticFilterOp` and
   `SemanticRowFilterOp` are not in the MVP. They go in a separate
   "constraints" view if there's demand.

## What this design does not include (deferred)

- **MCP `lineage` tool.** Gated on canonical model IDs.
- **OKF "Lineage" section** in concept docs. Gated on the same.
- **OpenLineage adapter.** Gated on canonical model IDs. Static
  lineage lacks the run/job/output-dataset namespace a complete
  OpenLineage event needs; a partial mapping would be misleading.
- **Cross-model impact analysis** ("if I change model X, which
  models break?"). Derived from `WorkspaceLineage.downstreamOf` —
  a small CLI subcommand, can ship in a follow-up.
- **Runtime / observability lineage.** Already covered by the audit
  log; this design is intentionally orthogonal.

## Effort estimate

| PR | Surface | LOC | Time |
|---|---|---|---|
| #207 | library + tests + this design doc | ~510 | 2-3 days |
| #208 | CLI subcommand (`semanticdf lineage …`) | ~80 | 1 day |

The library PR covers the data model, the SQL extractor, the 4
transforms, the JSON round-trip, the 4 unit test suites, and the
golden-fixture integration tests. The CLI PR is a small add-on.
