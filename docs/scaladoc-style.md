# Scaladoc style guide

The bar for Scaladoc comments in `io.semanticdf.*`. Use this when
adding or improving documentation. The goal: a reader who's never
seen the framework should be able to use a public type correctly from
the Scaladoc alone, without opening the source.

The canonical example of every rule below is `src/main/scala/io/semanticdf/Model.scala`
(Dimension, Measure, Transform, MeasureExtra). Read it once as a reference,
then write new Scaladoc the same way.

---

## The seven rules

### 1. Open with what it IS, not what it does

Don't start with "Constructs a time dimension" — that's the function's
name. Start with the *category* a reader needs to recognise:

```scala
/** A grouping field on a semantic model — the columns you `groupBy` on
  * or filter against. The "dimensions of a cube": `carrier`, `region`,
  * `order_date`, `customer_id`. */
```

```scala
/** A numeric aggregate on a semantic model — the values you compute
  * across groups. `flight_count`, `total_revenue`, `avg_distance`. */
```

The opening sentence should let a reader decide "is this the thing I
need?" without reading further.

### 2. Show, don't tell — include a `{{{...}}}` example

For any non-trivial type, include at least one copy-pasteable example
in a `{{{...}}}` block. The example must include all imports a
newcomer would need, and must not use undefined variables.

**Good** (compiles after `import`):

```scala
/** Construct a plain (non-time, non-entity) dimension.
  *
  * {{{
  * import io.semanticdf._
  *
  * val carrier = Dimension("carrier", t => t("carrier"))
  * }}}
```

**Bad** (example uses undefined `t`, `sum`):

```scala
/** Construct a dimension.
  *
  * {{{
  * val m = Measure("revenue", t => sum(t("amount")))  // where do `t`, `sum` come from?
  * }}}
  */
```

If the example needs an active `SparkSession`, write it as a comment
("with `implicit val spark: SparkSession = ...`") rather than guessing
the user's setup.

### 3. Use `@param` only when meaning isn't obvious

Don't `@param name the name` — the reader can see that. Use `@param`
when the meaning, allowed values, or constraints aren't obvious from
the type signature:

```scala
/** ...
  *
  * @param smallestTimeGrain the finest grain allowed — one of `"year"`,
  *                          `"quarter"`, `"month"`, `"week"`, `"day"`,
  *                          `"hour"`, `"minute"`. `None` means no restriction.
  * @param isEventTimestamp `true` if this column is the table's event-time
  *                          column. Reserved for the future streaming terminal
  *                          (ADR 0002); no effect in batch today.
  */
```

For boolean flags, always state what `true` vs `false` does. Never
`@param isEntity whether it's an entity`.

### 4. Name the actual exception type, not "raises a clear error"

Spell out what exception class, what condition triggers it, and when
(class-load time? query time? compile time?).

```scala
/** Requesting a finer grain than `smallestTimeGrain` (e.g. `query(timeGrain = "hour")`
  * against `Some("day")`) raises `IllegalArgumentException` at query time. */
```

```scala
/** Throws `IllegalStateException` if the SparkSession has been stopped. */
```

### 5. Qualify future / unshipped features

If you mention a feature that isn't in the current release, qualify
it. Readers who can't find the feature need to know whether it's
real-but-future or whether they misread the doc.

```scala
/** Reserved for the future streaming terminal (ADR 0002); no effect in
  * batch today. */
```

```scala
/** The `Dataset[T]`-shaped `query[T]` variant is on the roadmap; today
  * the typed path returns `Seq[T]` via `collectAs[T]`. */
```

Cross-reference ADRs and roadmap docs by name where applicable.

### 6. Cross-reference with `[[Foo]]`, but don't over-link

- `[[Dimension]]` and `[[SemanticScope]]` — yes, these are real types the reader will hit
- `[[SemanticTable.withDimensions]]` — only if the reader is about to need that method
- `[[apply]]` from a companion object — usually pointless; the example shows the call

If the linked name doesn't resolve in the file, drop the brackets
and use a plain code span instead: `Dimension.apply` reads fine.

### 7. Don't document the obvious

Skip Scaladoc on:

- **`equals` / `hashCode` / `toString`** — readers know what these do. Exception: write a brief note when the implementation is non-obvious (e.g. `Dimension.equals` compares `expr.toString` because functions have no value equality).
- **Constructor `val` params** of a `class` — they're covered by class-level `@param` tags. Don't repeat on each parameter.
- **Local `val`s inside method bodies** — they're not part of the public surface.
- **`private[semanticdf]` and `private` members** — internal implementation. Keep them documented only if the *next reader* needs them to maintain that code.

---

## Do / don't

**Don't** write Scaladoc that rephrases the type signature:

```scala
// BAD — adds nothing
/** The name. */
def name: String
```

**Don't** use undefined jargon:

```scala
// BAD — "column producer", "synthetic copy", "distinct-count subjects" are all jargon
/** @param expr a column producer — a function from [[SemanticScope]] to a Column.
  *             The scope exposes base columns and other measures via t. */
```

**Don't** invent contract promises you can't enforce:

```scala
// BAD — vague "zero runtime cost"
/** @param metadata arbitrary key-value pairs. Has zero runtime cost — Spark
  *                never sees these values. */
```

**Do** open with a recognisable category sentence (rule 1) and follow
with an example (rule 2). The rest is detail.

---

## Before you commit, checklist

- [ ] Opens with "what this IS", not "what this does"
- [ ] At least one copy-pasteable `{{{...}}}` example for non-trivial types
- [ ] Example imports are shown explicitly
- [ ] `@param` only on params where the meaning isn't obvious from name + type
- [ ] Exception types named (`IllegalArgumentException`, not "raises an error")
- [ ] Future / unshipped features qualified ("reserved for the future X (ADR NNNN)")
- [ ] All `[[Foo]]` cross-refs resolve in this file
- [ ] No Scaladoc on `equals` / `hashCode` / `toString` / private members / local vals
- [ ] `mvn compile` clean, `mvn test` still 335/335 green

---

## When NOT to update this guide

This guide covers the **public API** (types and methods users call).
Don't add Scaladoc to:

- Examples in `src/main/scala/io/semanticdf/examples/` — they're demo code with `println`, not library API
- CLI tools in `src/main/scala/io/semanticdf/tools/` — they're CLI; their output is `println` to stdout
- `private` / `private[semanticdf]` members — internal implementation

These are correct as-is. Adding Scaladoc to them would be noise.
