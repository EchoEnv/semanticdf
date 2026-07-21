# SemanticDF — First Consumer Onboarding Plan

**Goal:** Surface real production gaps in semanticdf by running it against real data with a real workload. Bugs that only surface with messy data, real schemas, and real volumes. Not a production deployment — a structured soak test.

> **Status (2026-07):** Plan served its purpose — first consumer landed in
> `v0.1.3`. Specifically `examples/cli-consumer/` (`sdf` CLI, PR `#57`) is a
> standalone Scala binary that exercises the MCP / REST surfaces as a real
> client. Within its first round of probing it surfaced two issues, both
> fixed:
>
> 1. `order_by` over REST was broken (regression from PR `#54`'s Jackson
>    Scala module) — fixed in PR `#56`.
> 2. `describe_model` `expr` field serialised as opaque lambda addresses
>    (`io.semanticdf.YamlLoader$$$Lambda$...`) — fixed in PR `#58`; the
>    library now carries `exprString` on `Dimension`/`Measure` and the
>    handler prefers it.
>
> `sdf` continues to serve as a regression witness for the REST contract.
> The structured 3-week soak (this plan's §2–§6) was abandoned in favour of
> the lighter-weight "let `sdf` re-run whenever the contract changes"
> approach. The plan is retained as historical context for why
> `cli-consumer/` exists.

---

## 1. Selecting the First Consumer

Pick one team/use case. Not all consumers are equal at this stage.

### Criteria

| Factor | Why it matters |
|---|---|
| **Data has nulls, skew, type variation** | The test fixtures are clean. Real data breaks clean assumptions. |
| **Query complexity matches Tier 1–2** | Basic group-by + calc + joins. The streaming terminal ships; streaming workloads are valid. |
| **Consumer has tolerance for early-stage rough edges** | Error messages are improving but not polished. They need to be willing to report back. |
| **Consumer can be blocked for < 1 day** | When they hit a bug, you need to be able to respond quickly. |
| **Real business value at stake** | They won't invest effort in feedback if there's nothing riding on it. |

### Good candidates

- Internal BI/reporting team with a dashboard that currently uses raw SQL
- A data pipeline that aggregates across joined tables and has join fan-out issues today
- An analytics team that maintains multiple similar SQL queries and wants to unify them

### Bad candidates (for now)

- Streaming workloads with strict low-latency SLAs (the streaming terminal ships, but its operational status is "ready for evaluation, not v1 SLA")
- Multi-hop join chains (3+ tables) — edge cases unverified
- External customers (not ready to expose rough edges externally)

---

## 2. What to Give the Consumer

### Starter pack

```
/path/to/semanticdf/
  README.md                   ← runnable examples, API reference
  docs/
    adr/                     ← design decisions on record
  examples/                  ← (new) canonical examples per phase
```

### Create `examples/` with realistic examples

Each example should be a **complete, runnable Scala script** (not a test — a script they can adapt):

```
examples/
  01_flights_basic.sc         ← group-by, calc
  02_flights_pct_totals.sc   ← t.all() usage
  03_orders_lineitems_join.sc  ← join_many fan-out
  04_filters_routing.sc       ← where/having routing
  05_time_series.sc           ← Dimension.time + atTimeGrain
```

Each script: data setup → semantic model → 3–5 queries → expected output.

### Give them the Known Limitations doc

`docs/known-limitations.md` — written for a non-author. Before they start, they should know:

- Streaming terminal is supported but has known limitations — see `docs/known-limitations.md`
- `===` / `=!=` for comparisons (not `==` / `!=`)
- No metastore / view registration yet
- Division by zero → null (unless using `safeDivide`)
- Lambda purity required (no side effects)
- Decimal precision: Spark handles it, but test your specific precision requirements

---

## 3. What to Ask Them to Test

### Week 1: Smoke test (1–2 days)

Focus on **does it work at all** on their data.

1. **Load their real schema** — feed a real DataFrame into `toSemanticTable`. Does it construct? Do dimension/measure definitions work?
2. **Run 3 representative queries** — their most common aggregation patterns. Do results match their existing SQL?
3. **Null handling** — do their tables have nulls in dimensions/measures? Do results look right?
4. **Error message quality** — when something goes wrong, do they understand the error? (This is a big one — we know our errors, they won't.)

**Success criteria:** At least 1 query produces correct results. No silent wrong answers.

### Week 2: Edge cases (2–3 days)

Focus on **boundary conditions** that the test fixtures don't exercise.

1. **Division by zero** — do any of their calcs divide by a measure that could be 0? Does null vs 0.0 matter?
2. **Large result sets** — how does the output behave with 100K+ rows? Any OOM risk?
3. **Multi-measure queries** — do 5+ measures in one query all resolve correctly?
4. **Calc-of-calc chains** — do their most complex derived metrics work?
5. **Join with their data shapes** — does `join_many` work with their table sizes? Is fan-out prevention doing what they expect?

**Success criteria:** Edge cases are documented. At least one gap found and filed.

### Week 3: Performance (3–5 days)

Focus on **does it meet performance expectations**.

1. **Compare to raw SQL** — run the same query in raw Spark SQL and time it. Is semanticdf within 2× overhead?
2. **`explain(spark)` review** — ask them to run `explain(spark)` on 2–3 slow queries. Can they read the output? Is there an obvious plan issue?
3. **Shuffle / partition awareness** — do they understand why the plan looks the way it does?
4. **Large table joins** — does `join_many` at their scale (100M+ rows) stay within memory budgets?

**Success criteria:** Performance is acceptable, or a specific bottleneck is identified and fixable.

---

## 4. Feedback Process

### What to ask for in every bug report

```
Consumer Bug Report Template

User:          [name / team]
Date:          [when it happened]
Query:         [the semanticdf code that failed]
Error:         [what they saw]
Expected:      [what they expected]
Severity:      [blocking / workaround-exists / cosmetic]
Workaround:   [did they find one?]
```

### Severity triage

| Severity | Definition | Response time |
|---|---|---|
| **Blocking** | Produces wrong results silently | Fix within 24h or provide documented workaround |
| **Workaround exists** | Throws an error but they can route around it | Fix within 1 week |
| **Cosmetic** | Error messages unclear, docs missing | Fix within 2 weeks |

### Weekly sync

A 30-minute weekly check-in for the first month:
- What's working
- What's blocked
- Bugs filed this week
- Questions about the API

---

## 5. Go / No-Go Criteria

After the 3-week soak test:

### Go to internal beta (green light)

- Zero silent wrong results (every incorrect output was caught and fixed)
- Performance within 2× of raw SQL for 90% of queries
- Error messages are actionable for a non-author
- At least 3 real-world queries working end-to-end
- Known limitations list is accurate and complete

### No-go (not ready yet)

- Any silent wrong results found
- Performance unacceptable on core queries
- Consumer blocked for > 2 days by an unresolved bug
- API is confusing enough that the consumer can't use it without constant guidance

---

## 6. Parallel Work You Can Do While Soak Testing

While the consumer is soaking, improve the areas most likely to need work:

| Work | Why |
|---|---|
| **Metastore integration** (`createTempView`) | Consumers will want to register semantic tables for BI tools — easy to add |
| **Error message audit** | Audit every `throw new IllegalArgumentException` and score each one: 1 = cryptic, 5 = actionable. Fix the 1s and 2s. |
| **Calc-author guide** | Write `docs/calc-author-guide.md` — the purity contract, common pitfalls, `safeDivide` usage, performance tips |
| **Performance benchmarks** | Run `examples/` against your test fixtures with timing. Establish baseline. |
| **Known limitations doc** | Write `docs/known-limitations.md` before the consumer starts — saves them from hitting obvious gaps without context |

---

## 7. What's Not Covered in This Plan

- **Publishing to Maven Central** — that's a later step. First consumer is internal.
- **Multi-tenant security** — not designed in yet. First consumer should be a single team with shared data access.
- **Schema evolution** — if their source schema changes, there's no invalidation. That's a future feature.
- **Streaming** — the streaming terminal ships but its first-consumer fit is open. Producer/consumer-team operational stability (SLA, restart safety, source-of-truth for window/watermark choices) is the consumer's call; the library provides the model DSL + lifecycle-shape primitives.

---

*This plan is itself a living document. Update it after the first consumer completes each week.*
