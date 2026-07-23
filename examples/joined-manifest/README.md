# joined-manifest

Worked example showing **emitting and consuming a joined-manifest
using the v0.1.11 library API**.

## What this is

In v0.1.11 the `joined-models-manifest` recipe is fully implemented — per-side metadata round-trips **and** the `on` join key is reconstructed functionally from the wire shape (typed `join_on` entry / multi-key AND / SQL-form `onExprString` fallback). The example walks through:

1. Loading YAML model(s), driving a programmatic join via
   `SemanticTable.join_one` (which populates `SemanticJoinOp.leftSide` / `rightSide` for the per-side metadata).
2. Emitting `kind: "semanticdf-joined-manifest"` JSON via
   `SemanticManifest.toJoinedJson`.
3. Source-free header inspection via `SemanticManifest.parseJoinedMeta`.
4. Reconstructing a `SemanticTable` from the JSON via
   `SemanticManifest.fromJoinedJson`.

## Run

```bash
cd examples/joined-manifest
mvn -o package
mvn -o scala:run -DmainClass=com.example.joinedmanifest.Main
```

The run writes `target/joined-manifest.json` and prints the parsed
meta header. Round-trip is non-throwing; execution of the restored
join is fully functional — the `on` lambda is rebuilt from the wire keys.

## Compared to `joined-manifest-split`

| | `joined-manifest-split` | `joined-manifest` (this one) |
|---|---|---|
| Manifest emitter | hand-rolled string | `SemanticManifest.toJoinedJson` |
| Manifest reader | none (manifest was thrown away) | `SemanticManifest.fromJoinedJson` |
| Side metadata | composed in caller | automatic (via `SemanticJoinOp.leftSide`/`rightSide`) |
| Per-side id | manual `sideIdentity(parent, "left", ...)` | automatic (the writer calls `sideIdentity` internally) |
| Caveats | — | exposed via `parseJoinedMeta` (the merged-model caveats remain, see recipe doc) |

Use `joined-manifest` for canonical usage; reach for
`joined-manifest-split` only when you need a deliberately hand-rolled
wire shape.

## Note


The `on` join key **reconstructs functionally** from the wire in v0.1.11
via `SemanticManifest.fromJoinedJson`:

- single-column: `l(k)=r(k)`
- multi-key: `AND` over the per-side key pairs
- non-equi / asymmetric: falls back to the captured `onExprString`
  SQL form (only present when the writer captured one)

The `joined-keys` foundation (PRs #153 + #154, completed in v0.1.11)
and the Path C caveat closure (v0.1.12 — `extra_dimensions[]` /
`extra_measures[]` for alias-prefixed dims, `leftPrefix` / `rightPrefix`
on the `join` block) make this possible end-to-end. The reconstructed
model carries the per-side metadata + keys faithfully; for typical
equi-joins `restored.execute(spark)` works without re-loading from
YAML. The only remaining narrow caveat is non-equi / OR predicates,
which fall back to the captured `onExprString` SQL form — usable but
not equivalent to a re-loaded YAML.
