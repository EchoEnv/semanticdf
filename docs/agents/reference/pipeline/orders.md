---
type: SemanticTable
title: Orders
description: "Cleaned and enriched orders (silver layer from raw/orders_raw.csv)"
resource: file://examples/pipeline/models/orders.yml
timestamp: 2026-07-14T19:42:54Z
tags: [identifier, semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| customer_id | dimension | `customer_id` | Customer who placed the order (join key to customers) | owner=data-platform-team; tags=[identifier] |
| order_date | dimension | `order_date` | Date the order was placed | owner=data-platform-team |
| order_id | dimension | `order_id` | Unique order identifier | owner=data-platform-team; tags=[identifier] |
| product | dimension | `product` | Product name | owner=data-platform-team |
| status | dimension | `status` | Order status (shipped, pending, cancelled, returned) | owner=data-platform-team |
| order_count | measure | `count(1)` | Number of orders in this group | aggregation=count; owner=analytics-team; unit=count |
| total_revenue | measure | `sum(total_amount)` | Total revenue from this group | aggregation=sum; owner=finance-team; unit=USD |
| total_units | measure | `sum(qty)` | Total units sold in this group | aggregation=sum; owner=analytics-team; unit=count |

# Joins

- **[customers](customers.md)** — one, on \`customer_id = customer_id\`

# Calculated measures

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| avg_order_value | calc | `total_revenue / order_count` | Average order value in USD | owner=finance-team; unit=USD |
| pct_of_total_revenue | calc | `total_revenue / all(total_revenue)` | Fraction of all revenue from this group | format=percent; owner=finance-team; unit=ratio |

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

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](./mcp-contract.md) for the full schema.

# Citations

[1] [examples/pipeline/models/orders.yml](file://examples/pipeline/models/orders.yml) — the source schema this document references.
