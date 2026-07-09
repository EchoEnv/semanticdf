# semantica

A semantic layer for Apache Spark (JVM), adapted from the
[Boring Semantic Layer](https://github.com/boringdata/boring-semantic-layer) (Python/Ibis).

**Status:** Phase 2a — core model, group-by/aggregate, calc measures, calc-of-calc
chains with dependency auto-pull. See [`DESIGN.md`](DESIGN.md) for the full design and
[`docs/adr/`](docs/adr/) for recorded decisions.

## Build

Requires **JDK 17** and **Maven 3.9+**. Spark is on the classpath as `provided`
(it comes from your cluster/runtime).

```bash
mvn test
```

To build against Spark 4.x instead of the default 3.5.8:

```bash
mvn -Pspark4 test
```

## What exists

```scala
import io.semantica._
import org.apache.spark.sql.functions.{count, lit, sum}

val flights = toSemanticTable(flightsDf, name = Some("flights"))
  .withDimensions(
    Dimension("carrier", t => t("carrier")),
  )
  .withMeasures(
    Measure("flight_count",   t => count(lit(1))),
    Measure("total_distance", t => sum(t("distance"))),
    // calc measure: references other measures BY NAME — resolved against the
    // aggregated DataFrame, no expression-tree surgery (DESIGN §6.1).
    Measure("avg_distance_per_flight", t => t("total_distance") / t("flight_count")),
    // calc-of-calc: depends on another calc — auto-pulled, applied layer by layer.
    Measure("distance_index", t => t("avg_distance_per_flight") / lit(100.0)),
  )

val byCarrier: DataFrame = flights.groupBy("carrier")
  // request only the leaf calc — its deps (avg_distance_per_flight → total_distance,
  // flight_count) are pulled automatically.
  .aggregate("distance_index")
  .execute(spark)
```

- `toSemanticTable(df, name?, description?)` — construct a semantic model from a base `DataFrame`.
- `withDimensions(...)` / `withMeasures(...)` — immutable model extension.
- `groupBy(keys...).aggregate(measures...)` — group-by + aggregate, with **base measures**
  (run in the aggregation) and **calc measures** (resolved by name against the aggregated result).
- `.toDataFrame(spark)` / `.execute(spark)` — batch terminal (compile the op tree to a `DataFrame`).

Joins, the `query()` parameter API, percent-of-total, time semantics, and the streaming
terminal are defined in `DESIGN.md` and land in later phases (2–8).
