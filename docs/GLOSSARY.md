# Glossary

A reader's reference for terms-of-art used across the SemanticDF docs and
README. The library has a vocabulary of its own — every term below is
either a public API surface or a recurring concept you'll see in
examples, MCP tool responses, and Stack Overflow searches.

If you read something and it isn't here, file an issue. We add to this
list as new concepts land.

## Building blocks

- **SemanticTable** — the immutable facade over a base table + dimensions +
  measures. The thing you `toSemanticTable(df).withDimensions(...)` and
  then query. *Not* a Spark `DataFrame`; it compiles to one.
- **SemanticOp / op tree** — `SemanticTable` wraps a root `SemanticOp`
  (sealed trait). Each operation (`groupBy`, `aggregate`, `join`) adds a
  new node. Nothing runs until a terminal — `toDataFrame(spark)`,
  `execute(spark)`, or `collectAs[T](spark)` — compiles the tree.
- **Dimension** — a groupable / filterable attribute (carrier, region,
  timestamp). `Dimension("carrier", t => t("carrier"))` or
  `Dimension.time("ts", t => t("ts"))`. The `expr:` lambda maps the
  base DataFrame to a Spark `Column`.
- **Measure** — a numeric aggregate (`flight_count`,
  `total_passengers`). Three kinds:
  - *Base* — aggregates a column directly (`sum(t("distance"))`).
  - *Calc* — derives from other measures (`t("total") / t("count")`).
    References resolve by name against the aggregated DataFrame.
  - *Calculated measure (YAML)* — written as a string in YAML models
    (`total_passengers / flight_count`); compiled by `CalcExpr`.
- **Filter / row filter** — a pre-join hygiene predicate on the model's
  source DataFrame. Defined in YAML's `filters:` block.
- **Where / having filter** — a query-time predicate; the framework
  routes dimension refs to WHERE (pre-agg) and measure refs to HAVING
  (post-agg).
- **Pre-aggregation / post-aggregation** — the engine executes some
  operations before a `groupBy().agg(...)`, others after. "Pre-agg"
  means on the base DataFrame; "post-agg" means on the already-grouped
  result.

## Lambdas, scopes, and `t.`

- **SemanticScope** — the proxy `t` passed to dimension and measure
  lambdas. `t("carrier")` resolves a column; `t("flight_count")` resolves
  a measure (during calc compilation); `t.all("revenue")` references the
  percent-of-total row.
- **BaseScope** — used for base measures; only resolves base columns.
- **MeasureScope** — used for calc measures; resolves base columns
  *and* other measures by name.
- **`t.all(name)`** — the percent-of-total escape hatch. Returns the
  cross-joined totals row's column for `name`. Implemented in the
  compiler as `crossJoin(broadcast(totalsDf))` where `totalsDf` is the
  same measures aggregated with no group-by.
- **Expression-tree surgery** — the (rejected) approach of rewriting a
  Catalyst `Column` expression to inject a totals column reference. We
  sidestep this by resolving names at compile time instead of editing
  parsed expressions.

## Pipelines & joins

- **Op tree terminal** — the operation that compiles the tree to a
  `DataFrame` (or, in the future, a `StreamingQuery`). One model, one
  tree, two terminals (DESIGN §4.5).
- **`join_one` / `join_many` / `join_cross`** — the three join
  cardinalities. `join_many` pre-aggregates **both sides** at the join
  grain before joining, so you don't double-count. This is what makes
  multi-fact stars correct without trusting the SQL planner.
- **Pre-aggregation for fan-out prevention** — `join_many` runs an
  `agg` on both sides at the grain of the join keys before the
  join. Without this, a 1000-row fact table joined against a customer
  dimension would explode to ~customer_x_orders rows.

## Type safety

- **SemanticField[T] / SemanticDimension[T] / SemanticMeasure[T]** —
  phantom types and typeclass instances that wrap column references
  with their static role (`Dimension` vs `Measure`). The typed query
  API (`groupByDimensions(D1, D2)` / `aggregateMeasures(M1, M2)`)
  catches dimension-vs-measure confusion at compile time. Zero runtime
  cost.
- **Typed field-reference pattern** — the "declare once, use
  everywhere" pattern for typed field refs. Declare phantom tags
  (e.g., `sealed trait Carrier`) and implicit witnesses
  (`implicit val carrier: SemanticDimension[Carrier] =
  SemanticDimension.of[Carrier]("carrier")`) in a `Refs` object.
  Then use the typed refs at every call site:
  `model.groupByDimensions(carrier).aggregateMeasures(pax)`,
  `SortKey.asc(carrier)`, `where(carrier === "AA")`, etc. The
  implicit declaration line is the only place a field name is
  hard-coded. Catches dimension-vs-measure confusion and ref-name
  typos at compile time. See `docs/guide.md` for the worked
  walkthrough; see `examples/starter` Q8 for a working example.
- **Infix typed predicate** — the infix form `carrier === "AA"`,
  `pax > 500L`, `carrier.isNotNull`, `carrier isin Seq("AA", "DL")`,
  `carrier notin Seq(...)`, `carrier contains "AB"`,
  `tags arrayContains "vip"`, etc. enabled by the `PredicateOps._`
  import. The form takes any `SemanticField[T]` (the parent of
  `SemanticDimension` and `SemanticMeasure`) and delegates to the
  underlying `Predicate.Compare` / `Predicate.In` case classes. The
  field name is read from the typed ref's witness. The verbose form
  (`Predicate.Eq(carrier, "AA")`, `Predicate.In(carrier, Seq(...))`)
  continues to work unchanged. The string-specific ops
  (`contains`, `startsWith`, `endsWith`) compile to Spark's
  `Column.contains/startsWith/endsWith`; `arrayContains` compiles to
  `functions.array_contains`; `isin` / `notin` compile to
  `Column.isin(...)` (with a NOT for `notin`).

- **ResultDecoder[T]** — the typeclass that decodes a Spark `Row` into
  a typed value `T`. `collectAs[T]` plumbs a `Seq[T]` from the
  DataFrame.
- **ResultDecoder.derive[T]** — Scala 2 macro that auto-generates a
  `ResultDecoder[Foo]` instance for case classes whose fields are
  String/Int/Long/Double/Float/Boolean/Short/Byte/BigDecimal.
  Compile-time error on unsupported field types.
- **queryAs[T]** — typed-bundled-query terminal on `SemanticTable`.
  Same shape as `.query(...)` but returns a Spark `Dataset[T]`: the
  op tree is built, run, and each row is decoded into a `T` via the
  implicit `ResultDecoder[T]` and Spark `Encoder[T]`. The one-shot
  pick when you want a typed result without manually chaining
  `.toDataFrame → .collectAs[T]`. Requires `import spark.implicits._`
  for the encoder on case classes.
- **TypedColumn[T]** — phantom-typed value-class wrapper around
  `org.apache.spark.sql.Column`. The phantom `T` encodes the user's
  static type assertion about the column. Value class — zero
  allocation in the same compilation unit; one small allocation
  when crossing the library boundary. Implicit conversion to
  `Column` makes the typed form drop-in compatible with the
  untyped `SemanticScope => Column` lambda.
- **TypedSemanticScope** — the typed equivalent of `SemanticScope`.
  `t("col_name")` returns a `TypedColumn[T]` carrying the
  user-declared static type of the column. Used as the `t`
  parameter in `Measure.typed[T]` lambdas.
- **Measure.typed[T]** — typed measure factory. Same shape as the
  `Measure(name, fn)` constructor but the lambda is
  `TypedSemanticScope => TypedColumn[T]`, so the return type is
  type-checked at compile time. The typed form lowers to a plain
  `Measure` at runtime — works with `withMeasures(...)` and every
  downstream consumer. The phantom `T` is purely a compile-time
  check; at runtime, `T` is erased.
- **TypedArithmetic** — typed arithmetic functions for measure
  lambdas. `TypedArithmetic.divide[T, U, R](a, b)`,
  `plus[T, U, R]`, `minus[T, U, R]`, `multiply[T, U, R]`. Each
  function takes two `Column` args + implicit `Numeric[T]` /
  `Numeric[U]` / `Numeric[R]`; the compiler catches
  `divide[String, Long, Double]("a", "b")` at build time because
  `String` has no `Numeric` instance. Returns a `TypedColumn[R]`.
  The function body is just the corresponding Spark `Column` op
  (`/`, `+`, `-`, `*`) — type parameters are erased. Zero runtime
  overhead. Use together with `Measure.typed[T]` for fully
  type-checked measure lambdas.

## Validation & error envelopes

- **ExpressionValidator** — parses every `expr:` field in a YAML model
  with Spark's `CatalystSqlParser` at model-load time. A typo fails
  fast with a clear message naming the model, the field, the missing
  identifier, and the visible column set.
- **CalcExpr.validateReferences** — same idea for the
  `calculated_measures:` block; checks every `Ref(name)` and `all(name)`
  against the visible measure set at model-load time.
- **SparkFilterValidator** — enforces that row filters in YAML's
  `filters:` block reference pre-join columns only. Joins have not run
  yet at filter time, so joined-side columns are not visible.
- **Envelope / ErrorEnvelope** — the wire-format wrapper the MCP
  server (and REST transport) returns on every tool call. Standard
  shape: `{status, data, warnings, meta}` for success;
  `{error: {code, message, ...}}` for failure.
- **Error codes** — `MODEL_NOT_FOUND`, `INVALID_REQUEST`,
  `EXECUTION_ERROR`, `RESULT_TOO_LARGE`, `QUERY_TIMEOUT`,
  `AMBIGUOUS_DIMENSION` / `AMBIGUOUS_MEASURE`, and the wire codes used
  by the REST surface. See `semanticdf-mcp/src/main/scala/io/semanticdf/mcp/handlers/`
  for the handler that maps domain errors to these codes.

## YAML model & OKF

- **OKF (Open Knowledge Format)** — the markdown knowledge bundle
  under `docs/agents/reference/<example>/`. Generated by `okfgen`
  from each example's `models/*.yml`. The bundle is the source of truth
  for an LLM agent reading about a model.
- **`okfgen`** — the CLI tool that regenerates OKF bundles. The
  bundle is *checked in* (not generated at runtime) so the catalog is
  reproducible. Regenerate it whenever you change a YAML.
- **`introspect`** — the inverse of `okfgen`. Given a DataFrame path
  (parquet / csv / json), produce a *starter* YAML model. Heuristics:
  string columns become dimensions, numeric become measures,
  timestamp-keyed columns become time dimensions, names matching
  `_id` / `_key` become entity dimensions.
- **`status:` (YAML) / `ModelStatus` (Scala)** — lifecycle marker
  for a model. One of `draft` / `published` / `deprecated`. Defaults
  to `published` for back-compat with models that pre-date the field.
  Surfaces in MCP `describe_model` (`data.status`) and the manifest
  artifact (`model.status`). The library never fails queries on
  `Deprecated`; consumers (MCP server, agent framework) enforce policy.
  Wire-stable lowercase strings; renaming is a breaking change across
  MCP + manifest + YAML + Scala companion.

## Streaming terminal

- **`SemanticStreamingTableOp`** — the streaming counterpart to
  `SemanticTableOp`. Wraps a Spark `DataFrame` of a Structured
  Streaming source plus its dimensions/measures. The streaming
  root op; downstream `SemanticFilterOp`s, `SemanticAggregateOp`s,
  and `SemanticJoinOp`s hang off it the same way they do in batch.
- **`toStreamingSemanticTable(streamingDf, ...)`** — the factory
  for streaming models. Same builder pattern as `toSemanticTable`
  (`.withDimensions(...)`, `.withMeasures(...)`, `.groupBy(...)`,
  `.join_one(...)`, etc.); the only difference is the source
  argument is a streaming `DataFrame` returned by `spark.readStream`.
- **`toStreamingQuery(spark, opts)`** — the streaming terminal
  method, sibling of `.toDataFrame(spark)`. Validates the op tree
  against streaming constraints, builds the streaming
  aggregation/join pipeline, and starts a Spark `StreamingQuery`.
- **`StreamingQueryOptions`** — the parallel of `DataStreamWriter`
  options: `trigger`, `outputMode`, `checkpointLocation`,
  `foreachBatch` callback, plus `window` and `watermark` for the
  windowed-aggregation cases. Watermark defaults to the window
  column with a 10-minute delay when `window` is set; checkpoint
  defaults to a per-query temp dir.
- **`WindowSpec(column, duration)`** — time-window spec for
  windowed aggregation. `column` is the time column; `duration` is
  a Spark duration string (`"5 minutes"`, `"1 hour"`, ...).
  Required when the streaming model uses `groupBy + aggregate`.
- **`WatermarkSpec(column, delay)`** — event-time watermark. Same
  shape as `WindowSpec` but the `delay` is the *lateness tolerance*
  for late-arriving events.
- **`StreamingValidator`** — the streaming counterpart to the batch
  validator. Walks the op tree, collects violations (e.g., `orderBy`
  not supported, stream-stream joins not supported), and throws a
  `StreamingUnsupportedError` that names the offending pattern.
  Failing loudly at the terminal prevents silent wrong results.
- **`StreamingConfig`** — the declarative shape for starting a
  streaming query. Combines an `OutputSink` (noop / console /
  parquet / csv) with optional `WindowSpec`, `WatermarkSpec`,
  `outputMode`, and `checkpointLocation`. Operators translate a
  `StreamingConfig` into `StreamingQueryOptions` and pass it to
  `model.toStreamingQuery(spark, config)`. **The YAML `streaming:`
  block is intentionally not supported** — operational config lives
  in the operator's program, not in the model file.
- **`OutputSink`** — sealed trait for the streaming result sink.
  Variants: `Noop` (discard), `Console(limit)` (log sample),
  `Parquet(path)`, `Csv(path)`, `Custom(label, write)` (raw
  `DataFrame => Unit`; operator's program only — the YAML surface
  has no `streaming:` block to express any of these). Operators pick
  one per model; the framework turns the variant into the matching
  `foreachBatch` callback.

## MCP / REST surfaces

- **MCP** — Model Context Protocol. The wire protocol (JSON-RPC over
  stdio or HTTP) the MCP server speaks to give LLM agents tool access
  to the library. See `semanticdf-mcp/README.md`.
- **REST transport** — same `Handlers` and `Introspect` are exposed
  over JDK's `com.sun.net.httpserver` (no extra Spark dependency).
  Wire shape is identical to MCP; `Envelope[Data]` is the body for
  every endpoint.
- **Tools** — the operations MCP exposes: `list_models`,
  `describe_model`, `query`, `explain`, `introspect`. Per-tool request
  and response shapes are in `docs/agents/mcp-contract.md`.
- **`sdf`** — the standalone CLI consumer in `examples/cli-consumer/`.
  Not part of the server, but exercises the REST API as a real client.
  Useful as both an end-user tool and a regression witness.
