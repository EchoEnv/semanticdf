---
type: SemanticTable
title: Carriers
version: 1
description: "Airline carrier reference data (lookup)"
status: published
resource: file://examples/starter/models/carriers.yml
timestamp: 2026-07-21T18:59:26Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| carrier | dimension | `carrier` | Airline carrier code (IATA) | owner=data-platform-team |
| hub | dimension | `hub` | Primary hub airport | owner=data-platform-team |
| name | dimension | `name` | Full airline name | owner=data-platform-team |

# Filters

Pre-join row-level predicates on this model's source table. Applied automatically before joins.

| name | expr | description | metadata |
|------|------|-------------|----------|
| require_name_not_null | `name IS NOT NULL` | Carriers without a name are not part of the semantic layer. | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "carriers", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "carriers", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "carriers", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/starter/models/carriers.yml](file://examples/starter/models/carriers.yml) — the source schema this document references.
