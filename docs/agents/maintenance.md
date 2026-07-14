# OKF Maintenance — keeping the catalog in sync

The OKF (Open Knowledge Format) bundle under `docs/agents/reference/` is a
**checked-in, generated artifact**. Each example project's `models/*.yml` is the
**source of truth**. Whenever you change a YAML, you must regenerate the bundle
in the same PR or the catalog goes stale.

This document explains:

- Why the bundle is committed
- How to regenerate it
- What the drift check does
- What "drift" failure looks like
- Edge cases

---

## Why commit the generated bundle

Three reasons:

1. **Agents see the latest catalog without running anything.** An LLM consumer
   reading `flights.md` from the repo gets the current schema with no
   build/serve step.
2. **CI can catch drift instantly.** Without a checked-in bundle, drift is
   invisible — silently stale catalogs are the failure mode this prevents.
3. **The git diff is the audit.** Reviewers see exactly what changed in the
   catalog when a YAML change lands — same atomicity as code review.

---

## How to regenerate

```bash
make okfgen
git add docs/agents/reference
git commit -m "okf: regenerate reference bundle after <your change>"
```

`make okfgen` walks each `examples/*/models` directory, runs the generator into
`docs/agents/reference/<project>/`, and prints one line per project it
processed. It's idempotent — running it twice produces the same output.

---

## What the drift check does

`make okfgen-check` (also `make okfgen-check`) regenerates the bundle into a
**temp directory** and diffs it against the checked-in copy. If any file
differs, the command exits with status 1 and prints the diff (truncated to 80
lines).

CI runs `make okfgen-check` on every PR. A failing build means the bundle is
out of sync.

---

## What a drift failure looks like

```
okfgen-check: bundle is OUT OF DATE.
----- diff (truncated to 80 lines) -----
--- docs/agents/reference/starter/flights.md
+++ <temp>/starter/flights.md
@@ -10,6 +10,8 @@
 title: Flights
 version: 1
 description: "..."
+  filters:
+    require_origin_and_carrier:
 ...
----------------------------------------
Run 'make okfgen' locally, then 'git add docs/agents/reference && git commit' to fix.
make: *** [Makefile:35: okfgen-check] Error 1
```

Fix: regenerate the bundle, amend your commit, push.

---

## Edge cases

| Case | What to do |
|---|---|
| Added a new `examples/<name>/models/foo.yml` | Run `make okfgen` once. The bundle picks up the new file. |
| Renamed an example project (e.g. `examples/foo` → `examples/bar`) | Run `make okfgen`, then `git rm` the old `<project>/` dir under `docs/agents/reference/`. |
| Want to suppress drift checks temporarily (e.g. while iterating) | Don't disable the CI job. Locally, just don't run `make okfgen-check`. The local `git diff` is the source of truth while you iterate. |
| OKF generator has a bug and the diff is wrong | Fix the generator in `tools/OkfGen.scala`. `make okfgen-check` will tell you whether the new bundle is correct. |

---

## When NOT to use this

- **Schema changes**: don't put OKF regeneration in a pre-commit hook. The
  generator runs `mvn exec:java` which is slow. Run it explicitly in your PR
  workflow.
- **Inside `examples/` apps**: the apps themselves are consumers of the YAML;
  they don't need the OKF bundle to run. The reference bundle is for catalog
  consumers (MCP, agents, docs), not for the apps' own build.

---

## One-line mental model

> Edit YAML → run `make okfgen` → amend commit → push. If you forget, CI tells
> you, prints the diff, and points at this doc.
