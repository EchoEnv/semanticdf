# Unity Catalog OSS — local docker cluster

A **single-node Unity Catalog OSS cluster** with strict memory + disk caps,
used by the semanticdf-trino adapter's integration test for the
**first catalog adapter** (`UnityCatalogSourceResolver`).

This proves the multi-engine design's §4.6 layer-separation principle:
**a `SourceResolver` implementation can resolve a `SourceRef` against a
real Unity Catalog REST API**, regardless of which engine consumes the
result.

## Quick start

```bash
cd adapters/semanticdf-trino/docker-uc
# First time only: make the bind-mounted data dir world-writable
# so the container's `unitycatalog` user (UID 100) can write.
chmod -R 777 ./data
docker compose up -d                              # start UC
# Wait ~30s for the healthcheck to pass:
docker ps --filter "name=semanticdf-uc-test"      # Status: "Up ... (healthy)"

# (Optional) Run the memory + disk monitor in another terminal:
./monitor.sh

# Smoke test the REST API:
curl -s http://localhost:8089/api/2.1/unity-catalog/catalogs | python3 -m json.tool

# Run the integration test (gated by -Ddocker.tests=true):
cd ..
mvn -Ddocker.tests=true -pl . test \
    -Dtest='io.semanticdf.trino.integration.UnityCatalogIntegrationSpec'

# Clean up:
cd adapters/semanticdf-trino/docker-uc
./teardown.sh
```

## Resource budget (per user constraint: "always watch memory, disk first")

| Resource | Limit | Where it lives |
|---|---|---|
| Container memory (hard) | **1.5 GiB** | `docker-compose.yml` `deploy.resources.limits.memory` |
| Container memory (reservation) | 512 MiB | `docker-compose.yml` `deploy.resources.reservations.memory` |
| JVM heap (`-Xmx`) | **768 MiB** | `docker-compose.yml` `environment.JAVA_TOOL_OPTIONS` |
| Container CPUs (hard) | 2.0 | `docker-compose.yml` |
| Container CPUs (reservation) | 0.5 | `docker-compose.yml` |
| UC data + DB (bind mount) | `./data/` | `docker-compose.yml` `volumes` |

The JVM heap cap is independent of the container's RSS cap:
- **JVM cap (-Xmx768m)**: the JVM never asks the kernel for more than 768 MiB.
- **Container cap (1.5 GiB)**: OOM-kill if the JVM or native code exceeds 1.5 GiB.
- **Disk (./data/)**: bounded by host's free space; `monitor.sh` polls every interval.

`monitor.sh` writes per-tick resource usage to `monitor.log`:
```
[2026-08-05T22:30:00+00:00] tick=1 container=semanticdf-uc-test mem=412MiB cap=1536MiB disk=42MiB
[2026-08-05T22:30:10+00:00] tick=2 container=semanticdf-uc-test mem=438MiB cap=1536MiB disk=42MiB
```

## Why Unity Catalog OSS (not Glue / HMS / Nessie)

| Catalog | Local setup | Transport | Picked? |
|---|---|---|---|
| **Unity Catalog OSS** | ⭐⭐⭐⭐ (single Docker, built-in DB) | REST | **Yes** |
| AWS Glue | ❌ (cloud-only) | REST | No — user constraint |
| Hive Metastore | ⭐⭐⭐⭐⭐ (Derby + Docker) | Thrift | Backup option |
| Nessie | ⭐⭐⭐⭐ | REST | Could swap |
| LakeFS | ⭐⭐⭐⭐ | REST | Could swap |

Picked **Unity Catalog** because:
1. **REST transport** (matches the multi-engine design's REST-first preference per §4.6)
2. **Single-image setup** (no Postgres required for dev — built-in DB)
3. **No cloud credentials** (matches the user's "easy to setup locally not on-cloud")
4. **Cross-industry standard** (Databricks, Snowflake, AWS converging)

## Why a single-node setup (not HA)

The integration test only validates that a `SourceResolver`
implementation can resolve a `SourceRef` against a *real*
Unity Catalog REST API. We don't need HA — we need *a real UC*.
Single-node + built-in DB is the smallest config that:
- exercises the real UC REST API (catalogs, schemas, tables, columns)
- runs under tight memory caps (the user's hard requirement)
- persists to bind-mounted volumes (so the test can verify round-trip)

## Files in this directory

| File | Purpose |
|---|---|
| `docker-compose.yml` | UC service with memory caps + bind mounts + healthcheck |
| `config/server.properties` | UC config override (auth disabled, env=dev) |
| `monitor.sh` | Per-tick memory + disk usage reporter |
| `teardown.sh` | Idempotent shutdown + bind-mount wipe |
| `README.md` | This file |
| `data/` | Bind-mounted catalog data + DB (wiped on teardown) |

## Smoke-test the REST API

```bash
# List catalogs (UC ships with one default "unity" catalog):
curl -s http://localhost:8089/api/2.1/unity-catalog/catalogs | python3 -m json.tool

# Create a schema (for the integration test):
curl -X POST http://localhost:8089/api/2.1/unity-catalog/schemas \
  -H "Content-Type: application/json" \
  -d '{"name":"semanticdf","catalog_name":"unity"}'

# Create a table (for the integration test):
curl -X POST http://localhost:8089/api/2.1/unity-catalog/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name":"orders",
    "catalog_name":"unity",
    "schema_name":"semanticdf",
    "columns":[
      {"name":"id","type_name":"LONG","comment":"primary key","nullable":false},
      {"name":"region","type_name":"STRING","comment":null,"nullable":true},
      {"name":"amount","type_name":"DECIMAL","type_precision":18,"type_scale":2,"comment":null,"nullable":true}
    ],
    "storage_location":"file:///tmp/orders"
  }'

# Verify:
curl -s "http://localhost:8089/api/2.1/unity-catalog/tables/unity.semanticdf.orders" \
  | python3 -m json.tool
```