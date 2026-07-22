# joined-manifest

Worked example showing **emitting and consuming a joined-manifest
using the v0.1.11 library API**.

## What this is

In v0.1.11 the `joined-models-manifest` recipe became implementable (per-side metadata + keys)
in a narrow form (per-side metadata round-trips, but the `on` join key
(older: was previously BLOCKed at v0.1.10; now resolved — see "Implementation notes" below)). The example walks through:

1. Loading YAML model(s), driving a programmatic join via
   `SemanticTable.join_one` (which populates `SemanticJoinOp.leftSide`
   / `rightSide` thanks to the foundation in PR #150).
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


The `on` join key cannot be reconstructed from the wire — neither
`leftKeys[]` nor `rightKeys[]` are populated (the wire shape is
documented in the recipe, but the lambda shape of `SemanticJoinOp.on`
prevents automatic extraction). The restored model carries the
metadata side correctly; running `restored.execute(spark)` will throw
a clear pointer at the recipe's BLOCK §1 (legacy hand-rolled manifest, no keys — only legacy hand-rolled manifests hit this path).
Re-load from YAML to get a fully functional join.
