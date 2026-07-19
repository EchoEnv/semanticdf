# semanticdf hospital

Hospital data management + cleansing on top of semanticdf — the **full data-quality workflow** (ingest → profile → cleanse → load → query). Patient demographics, ALOS, 30-day readmission rate.

This template complements the other consumer templates ([starter](../starter/README.md), [pipeline](../pipeline/README.md), [window-analytics](../window-analytics/README.md), [customer-analytics](../customer-analytics/README.md), [operations-analytics](../operations-analytics/README.md), [telco-analytics](../telco-analytics/README.md)) by showing the **cleansing step** — most templates load clean data; this one starts with messy data and cleanses it in Scala before loading into semanticdf.

## What you get

```
semanticdf-hospital/
├── README.md                          ← you are here
├── pom.xml
├── data/
│   ├── patients_raw.csv               ← 11 rows with intentional quality issues
│   ├── patients_clean.csv              ← 9 rows after dedup + normalization
│   ├── encounters_raw.csv              ← 13 rows including a 30-day readmission for P007
│   ├── encounters_clean.csv            ← 13 rows, patient_ids remapped to canonical
│   └── diagnoses.csv                   ← 6 ICD-10 reference codes
├── models/
│   ├── patients.yml                   ← cleansed patient model
│   └── encounters.yml                  ← cleansed encounter model
└── src/main/scala/com/example/hospital/
    └── Main.scala                       ← full ETL → cleansing → queries workflow
```

## Run it (5 minutes)

### Prerequisites
- JDK 17, Maven 3.9+, Spark 3.5.x or 4.x

### Step 1: install semanticdf locally
```bash
cd /path/to/semanticdf
mvn install -DskipTests
```

### Step 2: run
```bash
cd examples/hospital
mvn scala:run -DmainClass=com.example.hospital.Main
```

You'll see all 5 steps run in sequence:
1. **INGEST** — load the raw CSVs (intentional data quality issues)
2. **QUALITY REPORT** — print counts of duplicates / missing values
3. **CLEANSE** — normalize names, dedup patients, fill missing MRNs
4. **SEMANTIC** — load the YAML models on the cleansed DataFrames
5. **QUERIES** — Q1 demographics, Q2 ALOS by department, Q3 30-day readmission rate

## The data quality issues (and how the cleanser handles them)

| Issue | How it's caught | Cleansing step |
|---|---|---|
| Mixed-case names (`john smith` vs `John Smith` vs `John A Smith`) | The quality report groups by lowercased name + dob | `initcap()` normalizes to Title Case |
| Duplicate patients (P001/P003/P004/P011 all "John Smith b. 1955") | Group count > 1 in the quality report | `dropDuplicates("first_name", "last_name", "date_of_birth")` |
| Duplicate MRNs (P001 and P004 both have `MRN-1001`) | Group count > 1 on mrn column | Remap to the canonical patient_id (the lowest) |
| Missing MRN (P006 has empty MRN) | `mrn IS NULL OR mrn = ''` filter | `coalesce(mrn, "MRN-GEN-" + id)` |

After cleansing: 11 patients → 9 unique patients; all MRNs filled; all names in Title Case.

## Sample output

The example prints a stage-by-stage trace via `Logger.info`. Expected output on the demo data:

```
INFO hospital: ======================================================================
INFO hospital: Hospital data management + cleansing — full ETL → semantic workflow
INFO hospital: ======================================================================
INFO hospital:   raw patients:    11 rows
INFO hospital:   raw encounters:  13 rows
INFO hospital:   diagnoses:        6 rows
INFO hospital: ======================================================================
INFO hospital: STEP 2: Data quality report
INFO hospital: ======================================================================
INFO hospital:   duplicate patients (same name+dob): 1   # 1 group (P001/P003/P004)
INFO hospital:   rows with missing/empty MRN:        1   # P006
INFO hospital:   duplicate MRN values:                2   # MRN-1001, MRN-1003
INFO hospital: ======================================================================
INFO hospital: STEP 3: Cleanse
INFO hospital: ======================================================================
INFO hospital:   raw patients:        11 rows
INFO hospital:   cleansed patients:   9 rows (after dedup)   # dropped P003, P004
INFO hospital:   encounters:          13 rows
INFO hospital: ======================================================================
INFO hospital: STEP 4: Build semantic models on the cleansed data
INFO hospital: ======================================================================
INFO hospital:   loaded models: encounters, patients
INFO hospital: ======================================================================
INFO hospital: STEP 5: Queries on the cleansed data
INFO hospital: ======================================================================
INFO hospital: --- Q1: Patient demographics (by gender, by insurance) ---
INFO hospital: --- Q2: Average length of stay (ALOS) by department ---
INFO hospital: --- Q3: 30-day readmission rate ---
INFO hospital:   patients with multiple encounters: 2   # P001 (2), P007 (3)
INFO hospital:   of which had a 30-day readmission:    1   # P007: E012 → E013 = 14 days
INFO hospital:   30-day readmission rate:             0.50
```

The 30-day readmission rate is `1 / 2 = 0.50`. P001 has two encounters but they're 31 days apart (just outside the 30-day window). P007 has three encounters — E005 (2024-02-20), E012 (2024-04-18), and the new **E013 (2024-05-02)**, which is **14 days after E012** → triggers `is_readmission = 1` → patient counted as readmitted.

## What it demonstrates

| Concept | Where it shows up |
|---|---|
| Data quality profiling (group counts, null detection) | STEP 2 |
| In-place Spark cleansing: `initcap`, `dropDuplicates`, `coalesce`, `monotonically_increasing_id` | STEP 3 |
| Loading cleansed in-memory DataFrames into semanticdf via `YamlLoader.loadDir(tables)` | STEP 4 |
| `groupBy(dim).aggregate(measure)` per group | All queries |
| Time-grain + `datediff` calc | Q2 — `avg_los` |
| `lag()` window per patient for readmission detection | Q3 |
| `countDistinct` (added in Scala for cross-group aggregation) | implicit throughout |
| Hybrid pattern: per-patient measures via semanticdf, final rate in Scala | Q3 — when the final aggregation crosses group boundaries |

## Related templates

- [`examples/starter`](../starter/README.md) — 7 basic queries (group-by, pct-of-total, joins, time-grain, filter, top-N window, MoM window)
- [`examples/pipeline`](../pipeline/README.md) — full BI lifecycle (raw → parquet → semantic). This hospital template shows the same lifecycle in-memory.
- [`examples/window-analytics`](../window-analytics/README.md) — top-N, MoM, running total
- [`examples/customer-analytics`](../customer-analytics/README.md) — RFM + cohort activity
- [`examples/operations-analytics`](../operations-analytics/README.md) — fulfillment + anomaly detection
- [`examples/telco-analytics`](../telco-analytics/README.md) — telco: ARPU, promotions, roaming
