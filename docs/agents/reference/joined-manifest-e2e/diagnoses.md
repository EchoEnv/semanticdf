---
type: SemanticTable
title: Diagnoses
version: 1
description: ICD-10 diagnosis reference table
status: published
resource: file://examples/joined-manifest-e2e/models/diagnoses.yml
timestamp: 2026-07-24T09:02:09Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| category | dimension | `category` | Broad clinical category | — |
| description | dimension | `description` | Plain-text diagnosis description | — |
| icd_code | dimension | `icd_code` | ICD-10 code (primary key) | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "diagnoses", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "diagnoses", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "diagnoses", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/joined-manifest-e2e/models/diagnoses.yml](file://examples/joined-manifest-e2e/models/diagnoses.yml) — the source schema this document references.
