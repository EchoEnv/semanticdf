---
type: SemanticTable
title: Plans
description: "Service plans offered (Basic, Standard, Premium, Family)"
status: published
resource: file://examples/telco-analytics/models/plans.yml
timestamp: 2026-07-21T18:59:26Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| plan_id | dimension | `plan_id` | Plan ID (primary key) | — |
| plan_name | dimension | `plan_name` | Plan name (human-readable) | — |
| target_segment | dimension | `target_segment` | Marketing target segment for the plan | — |
| avg_monthly_fee | measure | `avg(monthly_fee)` | Average monthly subscription fee across plans | — |
| plan_count | measure | `count(1)` | Number of plans | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "plans", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "plans", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "plans", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/telco-analytics/models/plans.yml](file://examples/telco-analytics/models/plans.yml) — the source schema this document references.
