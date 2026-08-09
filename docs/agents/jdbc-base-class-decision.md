# Decision: extract `semanticdf-jdbc-shared` base class?

**Status:** v1 — analytical. No code changes. The decision is documented
here and the doc is the deliverable.

**The TL;DR:** After reviewing the PostgreSQL adapter (~1,200 LoC, the
first true JDBC warehouse adapter — PR #430), the boilerplate that
*could* be extracted into a `semanticdf-jdbc-shared` base class is
**~250-350 LoC** out of ~1,200 total — about 25-30%. **My
recommendation: defer the extraction** until the second warehouse
adapter (Snowflake? BigQuery? Redshift?) lands. PG is too unique
(every adapter that ships will be) and the abstraction savings
aren't worth the coupling cost.

---

## What I looked at

The PostgreSQL adapter (PR #430, 7 main src files, 1,203 LoC):
- `PostgreSqlError.scala` (78 LoC) — sealed ADT, 12 cases
- `PostgreSqlClient.scala` (128 LoC) — boundary trait, 7 methods
- `JdbcPostgreSqlClient.scala` (312 LoC) — JDBC impl
- `PostgreSqlSourceResolver.scala` (97 LoC) — `extends SourceResolver`
- `PostgreSqlCatalogAdapter.scala` (259 LoC) — `extends CatalogAdapter`
- `PostgreSqlResultEncoder.scala` (76 LoC) — `MyPlatformResult → PortableQueryResult`
- `PostgreSqlEngine.scala` (253 LoC) — `extends Engine[Any]`

For comparison: `TrinoEngineProvider` (~100 LoC) is a thin wrapper over
`TrinoEngine` (~830 LoC) which has its own SQL syntax quirks
(Trino's `RenderRelOp` etc.). DuckDB is in-process — different
shape entirely.

## Per karpathy §2 — what boilerplate is actually shared?

### 1. The `*Error` sealed ADT — **mostly reusable, but per-driver**

Each driver has its own SQLState mapping (PG vs Snowflake vs MySQL
all differ). The **sealed-ADT shape** is reusable, but the **cases**
are per-driver:
- PG: 12 cases (ConnectionFailed, AuthenticationFailed, TableNotFound,
  ColumnNotFound, SyntaxError, UniqueViolation, CheckViolation,
  CasConflict, NetworkError, Interrupted, PoolExhausted, MalformedResponse)
- Snowflake: needs `OperationInProgress`, `StatementTimeout`,
  `NumericValueOutOfRange` (PG doesn't have these)
- BigQuery: needs `JobRateLimitExceeded`, `BackendError`

**Verdict:** the *pattern* is reusable (`sealed trait XError extends Product with Serializable` + SPECIFIC cases per `error-handling-style.md`). The *specific cases* are per-driver. Not worth extracting.

### 2. The `*Client` boundary trait — **mostly reusable**

The shape (7 methods, all `Either[Error, X]`) is reasonable. But each
driver has driver-specific methods:
- PG: `casUpdate(schema, table, expectedXmin, newContent)` (xmin-specific)
- Snowflake: `cloneTable(source, target)` (zero-copy clone, SNOWFLAKE-specific)
- BigQuery: `loadJobId` (load-job specific, no equivalent in PG)
- MySQL: `readUncommitted()` (no equivalent in PG)

**Verdict:** the SHAPE (trait with 7 methods returning `Either`) is reusable, but the SPECIFIC methods are per-driver. A base trait would have to be abstract, with adapters filling in the warehouse-specific parts. **Modest value** — saves ~50 LoC per adapter (the `describeTable` / `createTable` / `dropTable` boilerplate), but the warehouse-specific methods force the trait to be 50% abstract.

### 3. The JDBC impl (`getConnection` + `executeQuery` + `param binding`) — **reusable but JDBC-specific**

The pattern is clear:
```scala
getConnection().flatMap { conn =>
  try {
    val ps = conn.prepareStatement(sql)
    try {
      params.zipWithIndex.foreach { case (v, i) => ps.setObject(i + 1, v) }
      val rs = ps.executeQuery()
      try {
        // extract rows
      } finally rs.close()
    } finally ps.close()
  } catch {
    case e: SQLException => Left(sqlExceptionToError(e, "action"))
  }
}
```

This is **~80 LoC of JDBC boilerplate per method**. PG has 3 such
methods (executeQuery, casUpdate, getTableVersion). If we extract
to a base class, that's ~240 LoC saved. **High value IF we have
multiple warehouses.** But...

### 4. The `SourceResolver` — **reusable with a hook**

`DatabaseMetaData.getColumns` is the **standard JDBC API** for
describing a table. ALL JDBC warehouses support it (PG, MySQL, MSSQL,
Snowflake, etc.). The mapping from `ResultSet → ResolvedSchema` is
fully generic. **This is the strongest candidate for extraction** —
saves ~80 LoC per adapter.

### 5. The `CatalogAdapter` — **mostly per-driver**

The 3 publish modes + CAS pattern is reusable, but:
- The CAS mechanism is **completely driver-specific**:
  - PG: `xmin` system column
  - Snowflake: `LAST_DDL_TIME` + time travel
  - BigQuery: `etag` + job IDs
  - MySQL: `@@last_insert_id` + `version` column
- The DDL syntax differs per driver
- The mapping from `MyNameError → CatalogError` is per-driver

**Verdict:** the SHAPE (3 publish modes, CAS contract, typed errors) is
reusable. The MECHANISM (DDL syntax, version field) is per-driver.
A base class would force every adapter to implement the version
mechanism as an abstract method — which is what we already do.

### 6. The `Engine` SQL compiler — **completely per-driver**

The SQL dialect is per-driver. PG has its own syntax (LIMIT, OFFSET,
`::cast`, etc.). Snowflake has its own (SAMPLE, FLATTEN, etc.).
This is **NOT shared boilerplate** — it's the entire reason
different warehouses exist.

**Verdict:** no extraction possible. This is the bulk of the
adapter (~250 LoC in PG) and it MUST be per-driver.

## The math

| Component | LoC in PG | LoC extractable | % extractable |
|---|---:|---:|---:|
| `*Error` ADT | 78 | 30 (pattern only) | 38% |
| `*Client` trait | 128 | 60 (shape only) | 47% |
| JDBC impl boilerplate | 312 | 240 (param binding + connection mgmt) | 77% |
| `SourceResolver` | 97 | 80 (DatabaseMetaData.getColumns) | 82% |
| `CatalogAdapter` | 259 | 80 (publish-mode shape only) | 31% |
| `Engine` SQL compiler | 253 | 0 (dialect-specific) | 0% |
| ResultEncoder | 76 | 0 (engine-specific) | 0% |
| **Total** | **1,203** | **~490** | **~40%** |

So extraction **would** save ~490 LoC per new warehouse adapter. If
3 warehouses (PG, Snowflake, BigQuery) each save 490 LoC, that's
1,470 LoC saved total vs. ~500 LoC for the base class itself.

**BUT** — the savings is from per-driver + per-driver + per-driver
of writing similar-but-different code. The **coupling cost** is
significant: every new adapter must conform to the base class's
abstract methods, and changes to the base class ripple to all
adapters.

## My recommendation: **DEFER**

Per the original plan (*"wait for the first JDBC warehouse adapter
to commit before extracting"*") + per `karpathy §2` (minimum code
that solves the problem):

1. **The gate has been satisfied** (PG landed in PR #430) — we now
   have one warehouse adapter to learn from.
2. **The abstraction savings are real but not overwhelming** —
   ~490 LoC per new adapter, but only ~25-30% of the total
   adapter code.
3. **The cost is non-trivial** — coupling every future adapter to
   the base class's abstract methods.
4. **We need 2+ samples to see the pattern repeat** (per
   `scala-data-driven-refacer §3` "A rule becomes data only when
   it must change without a deploy"). With only 1 sample, we can't
   tell which parts of the "shared" code are coincidentally similar
   vs. actually shared.

**Wait for the second warehouse adapter** (Snowflake? BigQuery?
Redshift?). At that point we have 2 samples and can see the pattern
repeat.

## What I would do if extracting

If we DO decide to extract (after the second warehouse lands), the
proposed shape would be:

```
adapters/semanticdf-jdbc-shared/
└── src/main/scala/io/semanticdf/jdbc/shared/
    ├── JdbcClientBase.scala           # abstract JDBC impl: getConnection, param binding
    ├── JdbcSourceResolverBase.scala   # uses DatabaseMetaData.getColumns
    ├── JdbcCatalogAdapterBase.scala   # 3 publish modes; abstract CAS mechanism
    ├── JdbcEngineProviderBase.scala   # abstract SQL compile; delegates to subclass
    └── JdbcConnectionPoolFactory.scala # (already exists in Trino)
```

Each new warehouse adapter extends these + provides the
dialect-specific pieces (DDL syntax, version field, error mapping).
Estimated ~300-400 LoC for the base class + ~150-200 LoC per
warehouse adapter (vs. ~1,200 LoC today for PG).

## Alternative: shared REST helper (Direction B)

Same analysis applies to REST adapters (Hera + UC). ~60% of each
adapter is HTTP/auth/error-mapping boilerplate. But the same
gating logic applies: wait for the second REST adapter.

## Decision

**For v0.3.1: NO new code.** This doc is the deliverable. The
direction is deferred to v0.4.0+, gated on the second warehouse
adapter landing.

**Trigger conditions to revisit:**
1. A second JDBC warehouse adapter lands (Snowflake, BigQuery, etc.)
2. The "shared" code identified in this analysis turns out to be
   genuinely shared (not PG-specific coincidence)
3. A Snowflake or BigQuery adapter contributor expresses interest
   in the base class

When any of these fire, open a new PR with:
- Updated analysis comparing the second warehouse to PG
- Concrete `JdbcClientBase` / `JdbcSourceResolverBase` / etc.
- Estimated LoC savings per the second warehouse

Until then: **ship adapters as their own modules, following the PG
pattern**, and let the abstraction emerge from the second sample.

## See also

- [`adding-a-new-adapter.md`](adding-a-new-adapter.md) — the
  adapter authoring guide
- [`embedding-data-platforms.md`](embedding-data-platforms.md) — the
  in-process / unit-test patterns
- PR #430 — the PostgreSQL adapter (the first warehouse)
- `docs/design/v0.3.1-feature-parity-backlog.md` — the v0.3.1 scope

---

*Last updated: post-v0.3.1 (PG adapter landed). Awaiting second JDBC
warehouse adapter to revisit this decision.*
