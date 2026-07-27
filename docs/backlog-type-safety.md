# Backlog — type-safety extensions (typed predicates, streaming types, catalog metadata)

**Status:** Backlog idea. The original three-piece plan landed; this document now records the *deferred* items so they don't get lost. Pick up only when a real consumer signals demand.

The previous incarnation of this file was the typeclass work-plan that closed out v0.1.x. The three original pieces (typed query results, phantom-typed dimensions/measures, typed arithmetic in calc lambdas) all shipped. What's left is a small set of follow-ons; they're below.

## The pain

SemanticDF's earlier API was stringly-typed at the edges:

```scala
// All of these are runtime errors:
t("total_passengerrs")          // typo — fails at execute
t("avg_passengers") / t("count") // wrong type — crashes at execute
st.where("carrier" > 600)      // String > Int — runtime type error
st.aggregate("total_passengerrs") // typo in measure name — runtime
```

The runtime-error class shows up most clearly with a non-trivial model: a typo in one of a hundred dimension names passes review, lands in a model, surfaces as a Spark error only when an agent queries that dimension. The model can't catch it at compile time because the field names are strings.

Typeclasses were the chosen fix. Three pieces:

## What shipped

### Typed query results — `ResultDecoder[T]`

A typeclass that turns a Spark `Row` into a case class `T`. The library ships `ResultDecoder.derive[T]` (a Scala 2 blackbox macro) that synthesises the decoder from the case class's primary constructor. `SemanticTable.query[T]: Dataset[T]` returns a typed Dataset, eliminating the row-by-row `getString` / `getLong` boilerplate.

```scala
case class FlightCount(carrier: String, flight_count: Long)
val ds: Dataset[FlightCount] = st.query[FlightCount]
```

### Phantom-typed dimensions and measures — `SemanticDimension[T]` / `SemanticMeasure[T]`

A `SemanticField[T]` typeclass that carries a field name plus a phantom type `T` describing the column's Spark type. Passing a measure-typed ref to `groupByDimensions(...)` is now a compile error; passing a dimension-typed ref to `aggregateMeasures(...)` is also a compile error. Typos in field names are caught at the declaration site of the implicit val, not at every use site.

```scala
// Compile-time errors, not runtime:
groupByDimensions(pax_sum)             // measure where dim expected
aggregateMeasures(carrier)              // dim where measure expected
```

### Typed arithmetic in calc measures

A small typed arithmetic DSL within calc lambdas. `add[N1, N2, R]`, `divide[N, D, R]`, etc. carry the result type at compile time. Spark's own `Column` implicits are kept working for the untyped path so existing code is unaffected.

## What was deferred

- **Typed predicate filtering** — `"carrier" > 600` is a compile error only if the carrier dimension has a phantom type AND the predicate DSL infers the column type from the model. Possible but adds significant complexity; deferred until a consumer hits the case.
- **Implicit `Column → NumericExpr` conversions** — conflicts with Spark's own `Column` implicits; the explicit `divide(...)` form is clearer.
- **Phantom types on streaming models** — the streaming op-tree uses the same builders as batch, but `query[T]: Dataset[T]` doesn't fit the micro-batch model. Operators consume streaming results via the `foreachBatch` callback in `StreamingQueryOptions`.

## Status markers

Each piece above is fully wired and tested. The macro is Scala 2 (a Scala 3 port is out of scope for this design).
