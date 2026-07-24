# joined-manifest

Worked example showing **emitting and consuming a joined-manifest
using the latest library API**.

## What this is

The `joined-models-manifest` recipe is fully implemented — per-side
metadata round-trips **and** the `on` join key is reconstructed
functionally from the wire shape. Three reconstruction paths are
demonstrated end-to-end:

- **Equi-join** — `leftKeys` / `rightKeys` reconstruct `l(k) === r(k)`
  (and `AND` over pairs for multi-key).
- **Non-equi / OR / compound predicate** — the structured
  `predicate_ast` on the join block reconstructs the exact predicate
  via `Predicate.toColumn`. Tools get a typed view of the join
  condition; no SQL round-trip.
- **Prefixed producer** — `leftPrefix` / `rightPrefix` on the join
  block carry the alias prefixes through, so the reconstructed `on`
  lambda reads `l("L_id") === r("R_id"`).

The example walks through:

1. Loading YAML model(s), driving a programmatic join via
   `SemanticTable.join_one` (which populates `SemanticJoinOp.leftSide` /
   `rightSide` for the per-side metadata).
2. Demonstrating an equi-join (canonical case) — emit JSON with
   `leftKeys` / `rightKeys` and round-trip back.
3. Demonstrating a non-equi predicate (e.g. `l.date < r.valid_to`) —
   emit JSON with `predicate_ast` and round-trip back to the same
   predicate.
4. Demonstrating the prefixed producer — emit JSON with
   `leftPrefix` / `rightPrefix` on the join block.
5. Source-free header inspection via `SemanticManifest.parseJoinedMeta`.
6. Reconstructing a `SemanticTable` from the JSON via
   `SemanticManifest.fromJoinedJson`.

## Run

```bash
cd examples/joined-manifest
mvn -o package
mvn -o scala:run -DmainClass=com.example.joinedmanifest.Main
```

The run writes `target/joined-manifest.json` and prints the parsed
meta header. Round-trip is non-throwing; execution of the restored
join is fully functional — the `on` lambda is rebuilt from the wire.

## Compared to `joined-manifest-split`

| | `joined-manifest-split` | `joined-manifest` (this one) |
|---|---|---|
| Manifest emitter | hand-rolled string | `SemanticManifest.toJoinedJson` |
| Manifest reader | none (manifest was thrown away) | `SemanticManifest.fromJoinedJson` |
| Side metadata | composed in caller | automatic (via `SemanticJoinOp.leftSide`/`rightSide`) |
| Per-side id | manual `sideIdentity(parent, "left", ...)` | automatic (the writer calls `sideIdentity` internally) |
| Predicate shape | opaque `onExprString` SQL (legacy fallback) | structured `predicate_ast` (equi keys lattice for equi-join) |
| Caveats | — | exposed via `parseJoinedMeta` |

Use `joined-manifest` for canonical usage; reach for
`joined-manifest-split` only when you need a deliberately hand-rolled
wire shape (rare — for custom shapes that don't match the library's).

## Note

The `on` join key **reconstructs functionally** from the wire:

- **Equi (single-column):** `l(k) === r(k)`
- **Equi (multi-key):** `AND` over the per-side key pairs
- **Non-equi / OR / compound:** the `predicate_ast` on the join block
  reconstructs the exact predicate. See `Predicate.toColumn`.
- **Prefixed producer:** the `leftPrefix` / `rightPrefix` fields on the
  join block qualify columns in the reconstructed `on`.

The library supports four reconstruction paths from the wire (equi
keys lattice, structured `predicate_ast` for non-equi / OR, prefix
fields for aliased producers, asymmetric keys for differently-named
join columns). For typical models `restored.execute(spark)` works
without re-loading from YAML.
