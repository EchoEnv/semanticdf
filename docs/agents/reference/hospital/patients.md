---
type: SemanticTable
title: Patients
description: Cleansed patient master data
status: published
resource: file://examples/hospital/models/patients.yml
timestamp: 2026-07-21T18:59:26Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| city | dimension | `city` | Patient city | — |
| date_of_birth | dimension | `date_of_birth` | Date of birth | — |
| first_name | dimension | `first_name` | Patient first name (normalized to Title Case) | — |
| gender | dimension | `gender` | Patient gender (M or F) | — |
| insurance | dimension | `insurance` | Patient insurance carrier | — |
| last_name | dimension | `last_name` | Patient last name (normalized to Title Case) | — |
| mrn | dimension | `mrn` | Medical Record Number | — |
| patient_id | dimension | `patient_id` | Patient ID (primary key) | — |
| patient_count | measure | `count(1)` | Number of unique patients in the group | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "patients", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "patients", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "patients", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/hospital/models/patients.yml](file://examples/hospital/models/patients.yml) — the source schema this document references.
