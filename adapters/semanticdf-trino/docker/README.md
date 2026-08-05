# Trino decision-gate — local docker cluster

A **single-node Trino cluster** with strict memory caps, used by the
semanticdf-trino adapter's integration tests against a *real* Trino
cluster (the "decision gate" of the multi-engine design).

## Quick start

```bash
cd adapters/semanticdf-trino/docker
docker compose up -d                          # start Trino
# Wait ~30s for the healthcheck to pass:
docker ps --filter "name=semanticdf-trino"    # Status column: "Up ... (healthy)"

# (Optional) Run the memory + disk monitor in another terminal:
./monitor.sh

# Run the integration test (gated by -Ddocker.tests=true):
cd ..
mvn -Ddocker.tests=true -pl . test \
    -Dtest='io.semanticdf.trino.integration.*'

# Clean up:
cd adapters/semanticdf-trino/docker
./teardown.sh
```

## Resource budget (per user constraint)

| Resource | Limit | Where it lives |
|---|---|---|
| Container memory (hard) | **1.5 GiB** | `docker-compose.yml` `deploy.resources.limits.memory` |
| Container memory (reservation) | 512 MiB | `docker-compose.yml` `deploy.resources.reservations.memory` |
| JVM heap (`-Xmx`) | **768 MiB** | `config/jvm.config` |
| Per-node query memory | 256 MiB | `config/config.properties` `query.max-memory-per-node` |
| Per-node total memory | 384 MiB | `config/config.properties` `query.max-total-memory-per-node` |
| Per-data-node catalog data | 128 MiB | `config/catalog/orders.properties` |
| Container CPUs (hard) | 2.0 | `docker-compose.yml` |
| Container CPUs (reservation) | 0.5 | `docker-compose.yml` |

The arithmetic **must** fit: 384 MiB (per-node total) + system pool (~256 MiB) ≈ 768 MiB JVM heap. Trino enforces this at startup; misconfigured heaps are rejected.

## Why a single-node cluster (not multi-node)

The decision gate validates that the engine-portable `Model` compiles +
executes end-to-end against a *real* Trino cluster. Distributed query
planner code paths are out of scope for v1 — those land with the real
JDBC driver PR. Single-node:
- exercises the real Trino SQL planner
- exercises the real Trino JDBC driver
- runs under tight memory caps (the user's hard requirement)
- starts in ~30s, tears down cleanly

## What `monitor.sh` does

`monitor.sh` runs in a separate terminal, polling every 5s (override
via `MONITOR_INTERVAL_SEC`):

- **memory**: container RSS via `docker stats`; tracks peak in bytes + % of host
- **disk**: root filesystem usage + bind-mounted `./data` directory size
- **CSV log** to `monitor.log` (override via `MONITOR_LOG`)
- **summary** on Ctrl-C / SIGTERM with peak values

It's the user's "monitor memory, disk while running, to not explode
server" requirement, made literal.

## What's in `./data`

Bind-mounted from `./data/` to `/var/trino/data` inside the container.
Trino uses this for:
- `spill/` — query spill-to-disk when memory pool is exhausted
- `etc/` — Trino runtime metadata

`teardown.sh` wipes this on cleanup so disk usage doesn't accumulate
across test runs.

## Why no CSV sample data

The integration test bootstrap creates the table + inserts data via the
Trino JDBC driver at `beforeAll` time. A pre-loaded CSV would:
- commit us to a specific data shape
- need separate teardown logic
- make test failures ambiguous (data vs. code)

A `beforeAll` JDBC insert is self-contained in Scala — easier to read,
easier to debug, easier to expand.

## See also

- `../README.md` — module-level README; this cluster is the
  "decision gate" listed in the Open Items section
- `../src/test/scala/io/semanticdf/trino/integration/` — the integration
  test that uses this cluster
- `../../../../docs/design/multi-engine-design.md` §7.2 — the budget
  for this work
