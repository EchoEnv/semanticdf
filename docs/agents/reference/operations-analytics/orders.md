---
type: SemanticTable
title: Orders
description: Per-order data with fulfillment timestamps and amount
status: published
resource: file://examples/operations-analytics/models/orders.yml
timestamp: 2026-07-21T18:59:26Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| customer_id | dimension | `customer_id` | Foreign key to customers | — |
| order_date | dimension | `order_date` | Date the order was placed | — |
| order_id | dimension | `order_id` | Order ID (primary key) | — |
| status | dimension | `status` | Order status (e.g. shipped, processing) | — |
| order_amount | measure | `sum(amount)` | Total order amount in the group | — |
| order_count | measure | `count(1)` | Number of orders in the group | — |
| total_on_time | measure | `sum(on_time_flag)` | Sum of on_time_flag across orders (rate = total / order_count) | — |
| total_ship_days | measure | `sum(ship_days)` | Sum of ship_days across orders (avg = total / order_count) | — |

# Calculated measures

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| avg_ship_days | calc | `total_ship_days / order_count` | Average days from order to shipment | owner=ops-team; unit=days |
| on_time_rate | calc | `total_on_time / order_count` | Fraction of orders shipped within 2 days (0.0–1.0) | owner=ops-team; unit=ratio |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "orders", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "orders", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "orders", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/operations-analytics/models/orders.yml](file://examples/operations-analytics/models/orders.yml) — the source schema this document references.
