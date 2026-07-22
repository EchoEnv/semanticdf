# joined-manifest

Worked example showing **emitting and consuming a joined-manifest
using the v0.1.11 library API**.

## What this is

In v0.1.11 the BLOCKed `joined-models-manifest` recipe is implementable
in a narrow form (per-side metadata round-trips, but the `on` join key
cannot be reconstructed — see BLOCK §1). The example walks through:

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
join would throw (BLOCK §1) because the `on` lambda has no wire form.

## Compared to `joined-manifest-split`

| | `joined-manifest-split` | `joined-manifest` (this one) |
|---|---|---|
| Manifest emitter | hand-rolled string | `SemanticManifest.toJoinedJson` |
| Manifest reader | none (manifest was thrown away) | `SemanticManifest.fromJoinedJson` |
| Side metadata | composed in caller | automatic (via `SemanticJoinOp.leftSide`/`rightSide`) |
| Per-side id | manual `sideIdentity(parent, "left", ...)` | automatic (the writer calls `sideIdentity` internally) |
| Caveats | require user to know the BLOCK shape | exposed via `parseJoinedMeta` and the BLOCK citation |

Use `joined-manifest` for canonical usage; reach for
`joined-manifest-split` only when you need a deliberately hand-rolled
wire shape.

## Caveat (BLOCK §1)

The `on` join key cannot be reconstructed from the wire — neither
`leftKeys[]` nor `rightKeys[]` are populated (the wire shape is
documented in the recipe, but the lambda shape of `SemanticJoinOp.on`
prevents automatic extraction). The restored model carries the
metadata side correctly; running `restored.execute(spark)` will throw
a clear `IllegalStateException` with a pointer at the BLOCK finding.
Re-load from YAML to get a fully functional join.
