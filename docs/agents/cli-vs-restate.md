# CLI vs. Platform Restate — why `sdf` talks to Restate, not MCP

**Status:** design note, owner ship in CLI v0.3.1.
**Audience:** anyone reading `sdf` code and wondering why it has two clients
(Restate + MCP REST) instead of one.

## TL;DR

| Surface | Storage | Survives restart? | Multi-tenant? | Used by `sdf`? |
|---|---|---|---|---|
| **Platform Restate** (`semanticdf-platform`) | Postgres (`AuditEventStore`, `ModelStore`) | ✅ | ✅ | ✅ for `audit-tail` |
| **MCP REST** (`semanticdf-mcp`) | In-memory `Models`, in-memory `AuditSink` (1024-ring) | ❌ | ❌ | ⚠️ for `list` / `describe` / `query` / `explain` (legacy) |

`audit-tail` requires the durable surface (`--restate-url` / `$RESTATE_URL`).
The other four commands still talk to MCP REST because that's where the
legacy `Models` + `SemanticTable` engine lives today. That's a temporary
asymmetry — see "Next PR" below.

## Why doesn't `sdf` talk to MCP for everything?

Two reasons, in order of importance:

1. **Durability.** Per `scala-jvm-safety §3` (long-lived state) and
   `scala-error-handling §1` (errors are data), audit data is *long-lived*
   and *must* survive process restarts. Today, MCP's audit ring lives in
   memory and dies with the MCP process. Targeting the wrong surface for
   `audit-tail` would be a foot-gun: operators would assume durability
   that doesn't exist ("we have audit logs!") and lose history on every
   deploy. The CLI defaults to the durable surface so the safe path is
   also the easy path.

2. **Multi-tenancy.** The platform's `AuditService.queryRecent(tenant, ...)`
   is keyed by tenant. MCP's audit sink is a single global ring with no
   tenant concept. Routing through Restate is the only way to *support*
   multi-tenant audit; the CLI's `--tenant <id>` flag is meaningless
   against the MCP ring.

   (Today, the legacy `Models` + `SemanticTable` path is also
   single-tenant. The MCP REST surface that `list` / `describe` / `query`
   talk to is single-tenant, full stop. That's a known limitation of the
   legacy engine, not of the CLI. The CLI is correct to use the
   single-tenant surface for legacy queries; the platform's multi-tenant
   routing is an engine-level concern.)

## Other commands: planned to follow

The same logic applies to `list` / `describe` / `query` / `explain` — the
canonical surface is the platform, not MCP. They still talk to MCP REST
because the platform's query handler doesn't yet cover them. That changes
in the next PR.

## Next PR: MCP becomes a stateless proxy in front of Restate

**Goal:** `sdf` (and any other MCP-REST client) talks to one surface — the
platform. MCP keeps the stdio MCP transport for AI-agent consumers
(Claude Code, Cursor, etc.) but routes all state through Restate.

**What changes:**

1. **MCP loses its in-memory `Models` and `AuditSink`** (or keeps them
   only as a tiny local cache keyed by tenant + model version, populated
   from Restate on cold start).
2. **MCP routes every REST call to the corresponding platform handler**
   (`/models` → `ModelService.list`, `/query` → engine-portable path,
   `/audit` → `AuditService.queryRecent`).
3. **`sdf` drops the legacy `Client` (MCP REST) code path** and uses
   `RestateClient` for everything. The CLI becomes a single-client
   program again.
4. **MCP's stdio transport stays unchanged** — that's a separate concern
   (MCP protocol framing over JSON-RPC), and the stdio path is what
   AI agents use. The MCP server process just stops *owning* state.

**Why this is two PRs, not one:**

- This PR ships `audit-tail` because operators need durable audit *now*
  (today's MCP ring dies on every redeploy).
- The proxy rewrite is a larger blast radius — it touches every endpoint
  in `RestServer.scala` and every test in `RestServerSpec.scala`. Bisecting
  the two changes is cheaper than collapsing them.

**Acceptance criteria for the next PR:**

- `mvn -q -pl semanticdf-cli test` passes with the legacy `Client` removed
  from `Main.scala` (only `RestateClient` survives).
- `mvn -q -pl semanticdf-mcp test` passes with `Models` and `AuditSink`
  removed from `RestServer.scala` (or kept as a read-through cache).
- `sdf list` and `sdf audit-tail` both round-trip through the platform
  end-to-end against a fresh process boot — no in-memory state survives
  a restart of either side.

## See also

- `semanticdf-cli/README.md` — what `sdf` ships today.
- `docs/platform-architecture.md` — how the platform wraps state in
  Restate services.
- `docs/agents/mcp-contract.md` — the MCP REST wire shape that this PR
  starts to deprecate.
