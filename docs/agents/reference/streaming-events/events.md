---
type: SemanticTable
title: Events
description: "Real-time events arriving on the events topic, rolled up per 30-second window."
status: published
resource: file://examples/streaming-events/models/events.yml
timestamp: 2026-07-21T19:07:20Z
tags: [categorical, semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| event_type | dimension | `type` | Type of the event (e.g., 'heartbeat', 'deploy', 'error') | owner=streaming-team; tags=[categorical] |
| timestamp_bucket | dimension | `timestamp` | Event-time timestamp — driving column for windowed aggregation | — |
| event_count | measure | `count(1)` | Number of events (rows) arriving in the window | aggregation=count; owner=streaming-team; unit=count |
| total_value | measure | `sum(value)` | Sum of the value column for events arriving in the window | aggregation=sum; owner=streaming-team; unit=numeric |

# Filters

Pre-join row-level predicates on this model's source table. Applied automatically before joins.

| name | expr | description | metadata |
|------|------|-------------|----------|
| require_known_event_type | `type IS NOT NULL` | Drop events with null type — flagged by upstream QA rule. | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "events", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "events", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "events", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/streaming-events/models/events.yml](file://examples/streaming-events/models/events.yml) — the source schema this document references.
