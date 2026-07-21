---
type: SemanticTable
title: Promotions
description: "Active promotional offers (signup discounts, loyalty bonuses, etc.)"
status: published
resource: file://examples/telco-analytics/models/promotions.yml
timestamp: 2026-07-21T18:59:26Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| description | dimension | `description` | Human-readable description of the promotion | — |
| discount_pct | dimension | `discount_pct` | Discount percentage applied by the promotion | — |
| promo_code | dimension | `promo_code` | Promotion code (primary key) | — |
| avg_discount | measure | `avg(discount_pct)` | Average discount percentage across promotions | — |
| promo_count | measure | `count(1)` | Number of active promotions | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "promotions", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "promotions", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "promotions", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/telco-analytics/models/promotions.yml](file://examples/telco-analytics/models/promotions.yml) — the source schema this document references.
