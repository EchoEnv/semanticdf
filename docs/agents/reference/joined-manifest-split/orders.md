---
type: SemanticTable
title: Orders
description: "Orders enriched with customer details — joined model (anti-scope demo)"
status: published
resource: file://examples/joined-manifest-split/models/orders.yml
timestamp: 2026-07-22T10:30:53Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| customer_id | dimension | `customer_id` | Foreign key to customers.customer_id | — |
| order_date | dimension | `order_date` | Date the order was placed | — |
| order_id | dimension | `order_id` | Order ID (primary key) | — |
| order_amount | measure | `sum(amount)` | Total order amount (sum across rows in the group) | — |
| order_count | measure | `count(1)` | Number of orders in the group | — |

# Joins

- **[customers](customers.md)** — one, on \`customer_id = customer_id\`

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "orders", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "orders", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "orders", "dimensions": ["customers.name"], "measures": ["total_passengers"], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/joined-manifest-split/models/orders.yml](file://examples/joined-manifest-split/models/orders.yml) — the source schema this document references.
