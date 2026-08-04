---
type: SemanticTable
title: Customers
description: Cleaned customer reference data
status: published
resource: file://examples/pipeline/models/customers.yml
timestamp: 2026-07-21T19:07:20Z
tags: [identifier, pii, semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| city | dimension | `city` | Customer city | owner=data-platform-team |
| country | dimension | `country` | Customer country (ISO code, 2-letter) | owner=data-platform-team |
| customer_id | dimension | `customer_id` | Unique customer identifier (join key from orders) | owner=data-platform-team; tags=[identifier] |
| name | dimension | `name` | Customer full name | owner=data-platform-team; tags=[pii] |
| customer_count | measure | `count(1)` | Number of customers | aggregation=count; owner=analytics-team; unit=count |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "customers", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "customers", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "customers", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/pipeline/models/customers.yml](file://examples/pipeline/models/customers.yml) — the source schema this document references.
