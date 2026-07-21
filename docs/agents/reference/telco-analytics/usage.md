---
type: SemanticTable
title: Usage
description: "Per-customer usage events (calls, data, SMS) with plan + promotion context"
status: published
resource: file://examples/telco-analytics/models/usage.yml
timestamp: 2026-07-21T18:59:26Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| customer_id | dimension | `customer_id` | Customer who generated the event | — |
| customer_name | dimension | `customer_name` | Customer name (denormalized from customers table) | — |
| event_date | dimension | `event_date` | Date the usage event occurred | — |
| event_type | dimension | `event_type` | Type of usage: call, data, or SMS | — |
| is_roaming | dimension | `is_roaming` | true if event occurred outside the home network (premium rate) | — |
| plan_id | dimension | `plan_id` | Plan the customer is on (foreign key to plans) | — |
| plan_name | dimension | `plan_name` | Plan name (denormalized from plans table) | — |
| promo_code | dimension | `promo_code` | Promotion code applied to this customer (nullable) | — |
| usage_id | dimension | `usage_id` | Usage event ID (primary key) | — |
| avg_event_amount | measure | `avg(amount)` | Average revenue per usage event | — |
| event_count | measure | `count(1)` | Number of usage events in the group | — |
| total_revenue | measure | `sum(amount)` | Total usage revenue in the group | — |
| total_roaming_revenue | measure | `sum(case when is_roaming = true then amount else 0 end)` | Revenue from roaming events only (premium service) | — |

# Calculated measures

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| revenue_per_event | calc | `total_revenue / event_count` | Total revenue divided by event count | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "usage", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "usage", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "usage", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/telco-analytics/models/usage.yml](file://examples/telco-analytics/models/usage.yml) — the source schema this document references.
