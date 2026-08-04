---
type: SemanticTable
title: Flights
version: 1
description: "Flight facts: per-flight distance and passenger counts"
status: published
resource: file://examples/joined-manifest/models/flights.yml
timestamp: 2026-07-22T15:36:45Z
tags: [airline, airport, identifier, semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| carrier | dimension | `carrier` | Airline carrier code (IATA two-letter identifier) | owner=data-platform-team; tags=[airline, identifier] |
| flight_date | dimension | `flight_date` | Scheduled flight date | — |
| origin | dimension | `origin` | Origin airport code (IATA three-letter) | owner=data-platform-team; tags=[airport] |
| flight_count | measure | `count(1)` | Number of flights (rows) in the group | aggregation=count; owner=analytics-team; unit=count |
| total_distance | measure | `sum(distance)` | Total distance in miles across all flights | aggregation=sum; owner=analytics-team; unit=miles |
| total_passengers | measure | `sum(passengers)` | Total passengers across all flights in the group | aggregation=sum; owner=analytics-team; unit=count |

# Filters

Pre-join row-level predicates on this model's source table. Applied automatically before joins.

| name | expr | description | metadata |
|------|------|-------------|----------|
| require_origin_and_carrier | `origin IS NOT NULL AND carrier IS NOT NULL` | Drop rows with null origin or carrier — flagged in upstream QA rule. | — |

# Joins

- **[carriers](carriers.md)** — one, on \`carrier = carrier\`

# Calculated measures

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| avg_distance | calc | `total_distance / flight_count` | Average flight distance in miles | owner=analytics-team; unit=miles |
| avg_passengers | calc | `total_passengers / flight_count` | Average passengers per flight | owner=analytics-team; unit=count |
| pct_of_total | calc | `total_passengers / all(total_passengers)` | Fraction of all passengers served by this group | format=percent; owner=analytics-team; unit=ratio |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "flights", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "flights", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "flights", "dimensions": ["carriers.name"], "measures": ["total_passengers"], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/joined-manifest/models/flights.yml](file://examples/joined-manifest/models/flights.yml) — the source schema this document references.
