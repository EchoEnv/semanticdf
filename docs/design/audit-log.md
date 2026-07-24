# Audit log

**Status:** ACCEPTED — `io.semanticdf.audit.*` shipped in the post-v0.1.16
release.

## Problem

LLM agents running on top of the semantic layer make queries that humans
don't review line by line. Without an audit trail:

- You can't answer "what did my agent just query?"
- You can't detect the same query being run repeatedly (cache candidate).
- You can't tell whether the last agent run hit a timeout or an error.
- You can't measure per-query latency or result size.

The audit log closes that gap with a minimal, pluggable primitive.

## Design

The audit pipeline is intentionally tiny:

```
SemanticTable.query(...)  →  toDataFrame(spark)
                            │
                            ├── compile + execute
                            │
                            └── AuditEvent → AuditSink
                                              ├─ NoOp (default)
                                              ├─ JsonlStdout (logging)
                                              └─ InMemory (tests + MCP)
```

### `AuditEvent` (case class)

| Field | Source | Purpose |
|---|---|---|
| `ts` | `Instant.now()` at emit | When the query ran |
| `model` | `SemanticTable.name` or `sourceTable` | Which model |
| `measures` | captured at `query()` time | What was requested |
| `dimensions` | captured at `query()` time | What was requested |
| `whereHash` | `PredicateHasher.hash(where)` | Stable filter fingerprint |
| `havingHash` | same, for `having` | Stable filter fingerprint |
| `rowCount` | left at 0 (no eager collect) | Reserved; consumers extend |
| `elapsedMs` | `nanoTime` around the compile | Wall-clock cost |
| `status` | `"ok"` or `"error"` | Outcome |
| `error` | `s"${class}: ${msg}"` on error | What went wrong |
| `requester` | `None` by default; the MCP layer can set it | Free-form agent id |
| `requestId` | `None` by default; the MCP layer can set it | Per-call correlation |

The fields are intentionally minimal. `orderBy`, `limit`, `timeGrain`,
`timeRange` are not in the event — they don't help answer "is this the
same query as the last one?" If you need them, add a `collector hook`
on the sink and emit a follow-up event.

### `PredicateHasher`

Stable, canonical SHA-256 of a `Predicate` tree. The canonical form
walks the tree in prefix notation:

```
eq(carrier,'AA')
and(eq(carrier,'AA'), gt(distance, 500))
```

Children of `And` / `Or` are sorted by their canonical form, so
`A and B` and `B and A` produce the same hash. The canonical form is
**not** SQL — it's a small deterministic notation designed for
hashing. Using SQL would tie the hash to the wire format and make it
brittle to formatting changes.

The same hash works for the v0.1.16 wire `PredicateAst` and the
library `Predicate` form, because the canonicalization is structural
— independent of the construction path.

### Sinks

```scala
trait AuditSink {
  def emit(event: AuditEvent): Unit
}
```

Three default implementations:

- `AuditSink.NoOp` — drops every event. The default.
- `AuditSink.JsonlStdout` — JSON Lines on a dedicated `java.util.logging`
  logger (`io.semanticdf.audit.jsonl`). Greppable, `jq`-able, redirectable
  via standard logging config.
- `AuditSink.inMemory(maxEvents)` — retains the last N events in
  arrival order, intended for tests and the MCP `audit_log` retrieval
  tool.

Sinks must:
- Be fast (audit is on the hot path of every query).
- Be non-throwing (an audit failure must never break a query).
- Be cheap to construct (typically shared across the SparkSession).

### Wiring

The audit hook is opt-in via a fluent setter:

```scala
val t = toSemanticTable(df, name = Some("flights"))
  .withDimensions(...)
  .withMeasures(...)
  .withAuditSink(AuditSink.JsonlStdout)  // <-- the only opt-in

t.query(measures = ..., dimensions = ..., where = ...)
  .toDataFrame(spark)  // <-- emits the event
```

The sink survives the fluent chain (`.query(...).limit(...).toDataFrame(...)`)
because the chainable methods preserve `auditSink` and `auditRequest`.
The request shape is captured at `query()` time so the audit event
carries the user's original intent, not the post-chain op tree.

The default is `None` (no audit), so existing call sites are
zero-overhead: a single `if (auditSink.isEmpty)` guard short-circuits
the audit path.

## What's NOT in v1

- **Eager row count.** Counting would force `df.count()` (a re-run
  of the query plan) on every emit. Not minimum code; not the user's
  request. The field is reserved (default `0`); consumers extend if
  they need it.
- **MCP `audit_log` tool.** A separate PR; the sink is in place to
  support it.
- **Async / queue-based sinks.** A follow-up if real-world sinks
  become a bottleneck.
- **Predicate AST shape parity.** The library's `Predicate` and the
  MCP `PredicateAst` both hash through `PredicateHasher.canonicalize`,
  but the canonical form is library-internal. Exposing it on the
  wire is a v2 conversation.

## Tests

19 tests in `AuditSpec`:

- `PredicateHasher` (10 tests): same-predicate, different-value,
  different-op, And/Or commutativity, And vs Or, nested stability,
  In order-insensitivity, In vs not_in, canonicalize form.
- `AuditSink` (4 tests): NoOp, InMemory retain order, InMemory
  overflow eviction, InMemory clear.
- End-to-end (5 tests): captured request shape, where hash, chain
  preservation, error path with status="error", no-sink default-off
  path.
