---
type: SemanticTable
title: Clinical Encounters
version: 1
description: Patient encounters enriched with ICD-10 descriptions and categories
status: published
resource: file://examples/joined-manifest-e2e/models/clinical_encounters.yml
timestamp: 2026-07-24T09:02:09Z
tags: [semantic-table]
---

# Schema

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| admission_year | dimension | `year(admission_date)` | Year of admission (derived time dimension) | — |
| department | dimension | `department` | Hospital department | — |
| discharge_status | dimension | `discharge_status` | How the patient left the hospital | — |
| encounter_id | dimension | `encounter_id` | Encounter ID (primary key) | — |
| primary_diagnosis | dimension | `primary_diagnosis` | ICD-10 code of the primary diagnosis (join key to diagnoses.icd_code) | — |
| encounter_count | measure | `count(1)` | Number of encounters in the group | — |
| expired_count | measure | `sum(case when discharge_status = 'expired' then 1 else 0 end)` | Number of encounters that ended in mortality | — |
| total_los | measure | `sum(datediff(discharge_date, admission_date))` | Sum of length-of-stay days across encounters in the group | — |

# Joins

- **[diagnoses](diagnoses.md)** — one, on \`primary_diagnosis = icd_code\`

# Calculated measures

| name | kind | expr | description | metadata |
|------|------|------|-------------|----------|
| avg_los | calc | `total_los / encounter_count` | Average length of stay in days | — |

# Examples

A consumer pointed at this catalog can run any of the following MCP `query` payloads:

```json
{"model": "clinical_encounters", "dimensions": ["carrier"], "measures": ["flight_count"]}
```

```json
{"model": "clinical_encounters", "dimensions": ["carrier"], "measures": ["total_passengers"], "where": [{"type": "ge", "field": "distance", "value": 500}]}
```

```json
{"model": "clinical_encounters", "dimensions": ["diagnoses.name"], "measures": ["total_passengers"], "limit": 10}
```

> Run via an MCP client pointed at this catalog. See [mcp-contract.md](../../mcp-contract.md) for the full schema.

# Citations

[1] [examples/joined-manifest-e2e/models/clinical_encounters.yml](file://examples/joined-manifest-e2e/models/clinical_encounters.yml) — the source schema this document references.
