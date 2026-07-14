---
type: SemanticTable
title: Flights
description: "Flight facts: per-flight distance and passenger counts over 3 months"
resource: file:///home/emilio/app/projects/semanticdf/examples/window-analytics/models/flights.yml
timestamp: "2026-07-14T19:42:54+00:00"
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| carrier | dimension | `carrier` | Airline carrier code (IATA two-letter) | — |
| flight_date | dimension | `flight_date` | Scheduled flight date | — |
| origin | dimension | `origin` | Origin airport (IATA three-letter) | — |
| flight_count | measure | `count(1)` | Number of flights in the group | — |
| total_distance | measure | `sum(distance)` | Total flight distance in miles | — |
| total_passengers | measure | `sum(passengers)` | Total passengers across all flights in the group | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "flights", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "flights", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "flights", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](./mcp-contract.md) for the full schema.

# Citations

[1] [examples/window-analytics/models/flights.yml](file://examples/window-analytics/models/flights.yml) — the source schema this document references.
