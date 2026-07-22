---
type: SemanticTable
title: Customers
description: Customer master data with signup dates and city
status: published
resource: file://examples/customer-analytics/models/customers.yml
timestamp: 2026-07-21T19:07:20Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| city | dimension | `city` | Customer city | — |
| customer_id | dimension | `customer_id` | Customer ID (primary key) | — |
| name | dimension | `name` | Customer full name | — |
| signup_date | dimension | `signup_date` | Date the customer signed up | — |

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

[1] [examples/customer-analytics/models/customers.yml](file://examples/customer-analytics/models/customers.yml) — the source schema this document references.
