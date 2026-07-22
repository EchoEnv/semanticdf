# Manifests and joins — a newbies' deep dive

This doc explains **why joined models can't be emitted as a single
manifest** in v0.1.11 (and what to do instead), in plain language for
operators who are seeing the writer throw for the first time. It also
walks through the worked example
[`examples/joined-manifest-split/`](../examples/joined-manifest-split/)
and links the design rationale back to the BLOCKed
[`joined-models-manifest`](design/joined-models-manifest.md) recipe.

---

## 1. What is a manifest, in one sentence?

A `SemanticManifest` is a portable JSON file that describes a model:
its dimensions, measures, filters, joins, name, version, status,
identity fields (added in v0.1.11), and a digest of what's inside. It
is **metadata only** — no computed results, no data.

```bash
tools.Main manifest --yaml models/orders.yml --id io.acme.warehouse.orders --out orders.json
```

The artifact is what you ship to a catalog, register in a registry, or
snapshot for change-tracking.

## 2. What is a "joined model" in semanticdf?

A `SemanticTable` is the runtime model object. Its internal `root` op
is one of:

- **`SemanticTableOp`** — the simple case: a single underlying
  DataFrame plus a list of dimensions, measures, filters, etc.
- **`SemanticJoinOp`** — the joined case: two sub-`SemanticTableOp`s
  joined on a key, with optional `extraDimensions` / `extraMeasures`
  that exist only post-join.

You build a joined model in YAML like this:

```yaml
orders:
  table: orders_csv
  status: published
  dimensions:
    order_id:    { expr: order_id }
    customer_id: { expr: customer_id }
    amount:      { expr: amount }
  measures:
    total_amount: { expr: sum(amount) }
  joins:
    customers:
      model: customers        # join model by YAML key
      type: one
      left_on: customer_id
```

When `YamlLoader` builds this, the resulting `SemanticTable` for
`orders` has `root = SemanticJoinOp(...)` — the YAML's "join" block
becomes part of the in-memory model.

## 3. Why does the writer refuse joined models?

`semanticdf`'s manifest format is single-table. The recipe at
[`docs/design/manifest-artifact.md`](design/manifest-artifact.md) §10
documents the anti-scope:

> Joined models (SemanticJoinOp root) are not supported. They need
> side metadata (per-side version / status / sourceTable / keys /
> cardinality) that the joined op doesn't carry today.

If you call `SemanticManifest.toJson(ordersJoinedModel, identity)`,
the writer throws:

```
java.lang.IllegalStateException:
  SemanticManifest.toJson: joined models (SemanticJoinOp root) are
  not supported. See docs/design/manifest-artifact.md §10.
```

The recipe acknowledges that this gap *might* close later via the
[`joined-models-manifest`](design/joined-models-manifest.md) design
recipe — but that recipe is currently **BLOCK** in
[`REVIEW-FEEDBACK.md`](design/REVIEW-FEEDBACK.md) §1, on five fatal
issues that mostly trace to `SemanticJoinOp` not carrying side
metadata. Until the BLOCK conditions resolve, joined models are
intentionally out of scope for the writer.

## 4. What to do today (the workaround)

Emit **one single-table manifest per side**, then optionally
hand-compose a joined envelope that mirrors the BLOCKed recipe's §3
shape. The `identity-bump` recipe (v0.1.11) added a helper specifically
to make this easy:

```scala
// Library — SemanticManifest.scala

val parentIdentity = SemanticManifest.Identity(
    id              = "io.acme.warehouse.orders",
    manifestVersion = SemanticManifest.InitialManifestVersion,
    namespace       = "prod",
    metadata        = Map("owner" -> "data-platform"),
)

// sideIdentity(parent, "left",  "customers") ⇒ identity for the left
// side, derived from the parent's FQN. The result has the parent's
// manifestVersion, namespace, and metadata (with an extra `side`
// entry), so per-side manifests align with the parent's governance.
val leftId  = SemanticManifest.sideIdentity(parentIdentity, "left",  "customers")
val rightId = SemanticManifest.sideIdentity(parentIdentity, "right", "orders")

// Each side is a single-table manifest and writes cleanly via toJson.
val leftJson  = SemanticManifest.toJson(customersTbl, leftId)
val rightJson = SemanticManifest.toJson(ordersTbl,   rightId)
```

The result is two well-formed `kind: "semanticdf-model-manifest"`
JSON files — fully round-trippable through `parseMeta` and
`fromJson`.

### Why per-side works, even though you can't call toJson on the joined root

Because the writer's check is on the **joined root**, not on what the
joined op itself contains. The sides of the join are still regular
`SemanticTableOp`s — single-table by construction. As long as you
emit a manifest for each side (not for the joined root), you're
inside the supported path. The post-join `extraDimensions` /
`extraMeasures` aren't lost; they live on whichever side has them
and travel through that side's manifest.

### Where do the per-side SemanticTables come from in practice?

The honest answer is: **you load each YAML independently**. Once
`YamlLoader.loadDir` has run, the joined root is in the map and the
underlying per-source tables aren't separately retrievable. So in
real code, you do:

```scala
val customersSource = YamlLoader.load("models/customers.yml", spark)("customers")
// Then a non-joined `orders-source.yml` (same columns as the
// orders table, no `joins:` block) gives you the right side.
val ordersSource    = YamlLoader.load("models/orders-source.yml", spark)("orders-source")
```

If you don't have a separate single-table source YAML for the right
side, **add one** — it should mirror the columns and shapes of the
source table the join consumes. That's the simplest fix; the BLOCKed
recipe's eventual native emit will eliminate this workaround.

## 5. The real path: `SemanticManifest.toJoinedJson` (v0.1.11)

As of v0.1.11, `SemanticManifest.toJoinedJson(model, identity)` emits a
real joined manifest on the wire — the hand-roll below is now only
needed if you need a custom envelope shape that the library doesn't
cover.

```scala
import io.semanticdf._
import io.semanticdf.SemanticManifest
import io.semanticdf.SemanticManifest.Identity

// Joined model — built via the public join_* API so the foundation
// populates SemanticJoinOp.leftSide / rightSide (PR #150).
val leftT  = toSemanticTable(leftDf,  name = Some("customers"))
val rightT = toSemanticTable(rightDf, name = Some("orders"))
val joined = leftT.join_one(rightT, (l, r) => l("customer_id") === r("customer_id"))

val identity = Identity(
  id              = "io.acme.warehouse.orders",
  manifestVersion = "0.1.0",
  namespace       = "prod",
  metadata        = Map("owner" -> "data-platform"),
)
val json = SemanticManifest.toJoinedJson(joined, identity, prettyPrint = true)
// -> kind: "semanticdf-joined-manifest" with embedded
//          model.left / model.right (single-table manifests)
//          and a model.join block with cardinality

// Round-trip:
val restored = SemanticManifest.fromJoinedJson(json, leftDf, rightDf)
// restored.isJoined == true; restored.joins.length == 1

// Source-free header:
val meta = SemanticManifest.parseJoinedMeta(json)
// meta.cardinality == "one", meta.leftDimensions / rightDimensions
// / mergedDimensions counts populated
```

The worked example `examples/joined-manifest/` walks through this flow
end-to-end.

**Caveat (BLOCK §1, still in effect):** the `on` join key cannot be
reconstructed from the wire — `leftKeys` and `rightKeys` are emitted
empty. The restored `SemanticTable` carries the metadata side fully,
but executing the join needs YAML or an explicit key list. The error
message on `restored.execute(spark)` points at the BLOCK finding.

If you still want to hand-roll a joined envelope (rare — for a custom
shape that doesn't match the library's), see §5.5 below.

## 5.5. Hand-rolling the joined envelope (rare; pre-v0.1.11 era)

If you need a custom envelope shape that the library doesn't cover
(e.g. a joined manifest in a v0.1.9 / v0.1.10 toolchain), you can still
compose a "joined bundle" of your own. Use the wire shape the BLOCKed
recipe's §3 proposes — that way, when the recipe unblocks and you
replace your hand-roll with `SemanticManifest.toJoinedJson(...)`, your
downstream tools' parsing code stays the same.

```json
{
  "schemaVersion": "v0.1.11-manifest",
  "kind":           "semanticdf-joined-manifest",
  "model": {
    "name":    "orders",
    "version": 0,
    "left":    { ... customers manifest ... },
    "right":   { ... orders   manifest ... },
    "join": {
      "cardinality": "one",
      "leftKeys":    ["customer_id"],
      "rightKeys":   ["customer_id"]

"joined bundle" of your own. Use the wire shape the BLOCKed recipe's
§3 proposes — that way, when the recipe unblocks and you replace
your hand-roll with `SemanticManifest.toJoinedJson(...)`, your
downstream tools' parsing code stays the same.

```json
{
  "schemaVersion": "v0.1.11-manifest",
  "kind":           "semanticdf-joined-manifest",
  "model": {
    "name":    "orders",
    "version": 0,
    "left":    { ... customers manifest ... },
    "right":   { ... orders   manifest ... },
    "join": {
      "cardinality": "one",
      "leftKeys":    ["customer_id"],
      "rightKeys":   ["customer_id"]
    }
  }
}
```

The `kind` discriminator lets future readers route correctly. Today
the only consumer is `SemanticManifest.parseMeta`, which doesn't know
`semanticdf-joined-manifest`; that gate is intentional — it keeps
the slot open for the BLOCKed recipe without committing to a schema.

## 7. The worked example

[`examples/joined-manifest-split/`](../examples/joined-manifest-split/)
walks through every step above in runnable form:

1. Loads `customer-analytics` YAMLs (one of which is joined).
2. Asserts `orders.isJoined == true` using the new public
   `SemanticTable.isJoined` accessor (no need to peek at
   `private[semanticdf]` fields).
3. Branches **before** calling the writer, so the demo never trips
   the anti-scope guard.
4. Calls `SemanticManifest.sideIdentity(parent, "left", "customers")`
   and `.sideIdentity(parent, "right", "orders")` to get per-side
   identities.
5. Calls `SemanticManifest.toJson` on each side independently.
6. Composes a hand-rolled joined envelope using the BLOCKed recipe's
   §3 shape — flagged with a comment that this section becomes
   `toJoinedJson(model)` once the recipe lands.
7. Writes `customers.json`, `orders.json`, and
   `orders.joined-envelope.json` to `target/manifests/`.

Run it:

```bash
cd examples/joined-manifest-split
mvn -o package
mvn -o scala:run -DmainClass=com.example.joinedmanifestsplit.Main
```

Expected output (last lines):

```
[demo] orders.isJoined == true; the writer (toJson) would throw (anti-scope §10). Branching on isJoined avoids the throw.
[demo] per-side customers manifest: 1328 bytes, id=io.example.joinedmanifestsplit.orders.customers
[demo] per-side orders manifest:    882 bytes, id=io.example.joinedmanifestsplit.orders.orders
[demo] customers manifest header: schemaVersion=v0.1.11-manifest kind=semanticdf-model-manifest id=io.example.joinedmanifestsplit.orders.customers namespace=demo dims=4 measures=0
[demo] orders manifest header: schemaVersion=v0.1.11-manifest kind=semanticdf-model-manifest id=io.example.joinedmanifestsplit.orders.orders namespace=demo dims=4 measures=2
[demo] hand-rolled joined envelope: 2706 bytes
[demo] artifacts written to .../target/manifests
[demo] === joined-manifest-split demo complete ===
```

Each per-side manifest passes through `parseMeta` cleanly
(`kind = semanticdf-model-manifest`, all identity fields populated).

## 8. Glossary (terms you'll see in the recipes)

| Term | What it means |
|---|---|
| `SemanticManifest` | The library object that reads/writes manifest JSON |
| `manifest` (noun) | The JSON artifact itself |
| `Identity` | The set of `id` / `manifestVersion` / `namespace` / `metadata` fields (added in v0.1.11, see [`docs/design/manifest-identity-bump.md`](design/manifest-identity-bump.md)) |
| `kind` | The discriminator field: `semanticdf-model-manifest` (single-table) or `semanticdf-joined-manifest` (joined; reserved for BLOCKed recipe) |
| `SemanticJoinOp` | The runtime op representing a join. Its sides are `leftRoot` / `rightRoot`, both `SemanticTableOp`s |
| Anti-scope | A deliberate non-feature. Documented in a recipe as "we are not going to support this" — usually with a rationale + a future ticket |
| BLOCK | A senior-engineer-review verdict meaning "this design needs fundamental work before it can be implemented" (vs 🟡 REVISE which means "tweak and resubmit") |

## 9. Where to look next

- [`docs/design/manifest-artifact.md`](design/manifest-artifact.md) —
  the v0.1.9 single-table manifest recipe (now implemented)
- [`docs/design/joined-models-manifest.md`](design/joined-models-manifest.md) —
  the BLOCKed joined-manifest recipe; explains why per-side is the
  workaround today
- [`docs/design/REVIEW-FEEDBACK.md`](design/REVIEW-FEEDBACK.md) —
  the BLOCK findings (the five fatal issues that gate
  `joined-models-manifest`)
- [`docs/design/manifest-identity-bump.md`](design/manifest-identity-bump.md) —
  the v0.1.11 recipe that adds `id` / `namespace` / `metadata` /
  `manifestVersion` / `$schema` (the recipe this PR implements)
- [`examples/joined-manifest-split/`](../examples/joined-manifest-split/) —
  the worked example this doc walks through

If you only have 30 seconds: **read §3 ("Why does the writer refuse
joined models?") and §4 ("What to do today"). The worked example
exists to make those 30 seconds stick.**
