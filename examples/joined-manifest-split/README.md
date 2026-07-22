# joined-manifest-split

> ⚠️ **Historical example (pre-v0.1.11).** As of v0.1.11, the BLOCK on
> `SemanticJoinOp.on` reconstruction was resolved by `SemanticManifest.toJoinedJson`
> (see `examples/joined-manifest/`). This example remains valid documentation for the
> per-side emit pattern and for consumers pinned to pre-0.1.11 versions.


A runnable worked example showing **how to emit per-side single-table
manifests from a joined YAML model** in semanticdf v0.1.11.

## What this is

Pre-v0.1.11 (and still useful as a reference), the `SemanticManifest.toJson` writer rejects
joined-rooted models (anti-scope per
[`docs/design/manifest-artifact.md`](../../docs/design/manifest-artifact.md)
§10). The
[`joined-models-manifest`](../../docs/design/joined-models-manifest.md)
recipe is BLOCKed on `SemanticJoinOp` not carrying enough side
metadata. Until that lands, operators who want a portable record of
a joined model emit one single-table manifest per side and
hand-compose the joined envelope themselves.

This example walks through that pattern, end-to-end:

1. Loads a directory of YAMLs (`models/`) that contains a joined
   model (`orders` joins `customers`).
2. Calls `SemanticManifest.sideIdentity(parent, "left", "...")` to
   derive a per-side `Identity` for the left side. Same for right.
3. Drives `SemanticManifest.toJson` on each side independently.
4. Validates each per-side manifest with `SemanticManifest.parseMeta`.
5. Hand-composes a `kind: "semanticdf-joined-manifest"` envelope
   matching the BLOCKed recipe's proposed wire shape — flagged
   clearly as a hand-rolled legacy alternative.

The conceptual walkthrough lives in
[`docs/manifests-and-joins.md`](../../docs/manifests-and-joins.md).
Read that doc first if this is your first time hitting
"joined models are anti-scope".

## Run

```bash
cd examples/joined-manifest-split
mvn -o package
mvn -o scala:run -DmainClass=com.example.joinedmanifestsplit.Main
```

The run prints the per-side manifest bytes and the joined-envelope
size, then writes three artifacts under `target/manifests/`:

```
target/manifests/
├── customers.json                  # single-table manifest (left side)
├── orders.json                     # single-table manifest (right side; stub)
└── orders.joined-envelope.json     # hand-rolled joined envelope
```

## What to look at

- `src/main/scala/com/example/joinedmanifestsplit/Main.scala` —
  every step has a comment. The `isJoined` guard at step 3 is the
  idiomatic way to branch on the writer's anti-scope — **no
  try/catch needed**.
- `docs/manifests-and-joins.md` — the long-form walkthrough.

## Notes

- The right-side manifest (`orders.json`) is a stub here because the
  demo ships a single YAML (`orders.yml`) with a `joins:` block — by
  the time you load it, `orders` is already joined. In a real
  project you'd ship a **separate, non-joined** `orders-source.yml`
  that mirrors the orders table alone, then use that for the right
  side's manifest. This is the simplest practical fix; the BLOCKed
  recipe's eventual native `toJoinedJson(model)` removes the need
  for it.
- The hand-rolled `kind: "semanticdf-joined-manifest"` envelope is
  forward-compatible with the BLOCKed recipe's §3 shape. When the
  recipe unblocks, replace the hand-roll with
  `SemanticManifest.toJoinedJson(model)` — your downstream parsing
  stays the same.
