---
type: SemanticTable
title: Encounters
description: "Patient hospital encounters (admissions)"
status: published
resource: file://examples/hospital/models/encounters.yml
timestamp: 2026-07-21T19:07:20Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| admission_date | dimension | `admission_date` | Date the patient was admitted | — |
| department | dimension | `department` | Hospital department (e.g. Cardiology, Pediatrics) | — |
| discharge_date | dimension | `discharge_date` | Date the patient was discharged | — |
| discharge_status | dimension | `discharge_status` | How the patient left (home, transferred, expired) | — |
| encounter_id | dimension | `encounter_id` | Encounter ID (primary key) | — |
| patient_id | dimension | `patient_id` | Foreign key to patients.patient_id | — |
| primary_diagnosis | dimension | `primary_diagnosis` | ICD-10 code of the primary diagnosis | — |
| encounter_count | measure | `count(1)` | Number of encounters in the group | — |
| expired_count | measure | `sum(case when discharge_status = 'expired' then 1 else 0 end)` | Number of encounters that ended in mortality | — |
| total_los | measure | `sum(los_days)` | Sum of length-of-stay days across encounters in the group | — |

# Calculated measures

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| avg_los | calc | `total_los / encounter_count` | Average length of stay in days | unit=days |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "encounters", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "encounters", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "encounters", "dimensions": ["carrier"], "measures": ["flight_count"], "order_by": [{"field": "flight_count", "direction": "desc"}], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/hospital/models/encounters.yml](file://examples/hospital/models/encounters.yml) — the source schema this document references.
