# Adding a New Adapter — semanticdf

**Status:** v1 — current. Audience: anyone adding a new engine
(`Engine[R]`), new source resolver (`SourceResolver`), or new catalog
adapter (`CatalogAdapter`) to the semanticdf library.

**The TL;DR:**

1. Create a new module under `adapters/semanticdf-<name>/` (copy the
   [`adapters/semanticdf-template/`](../adapters/semanticdf-template/) skeleton).
2. Add `extends Engine[Any]` + `extends SourceResolver` + `extends CatalogAdapter`
   for whichever ports you need. **Additive** — you never have to
   implement all three.
3. Follow the error-handling standard ([`error-handling-style.md`](../design/error-handling-style.md)).
4. Follow the data-driven mantra ([the file's section is short — read it once](https://scala-data-driven-refacer)).
5. Add tests using a hand-driven `Fake<X>Client` (per the UC/HMS/Hera pattern). Real
   integration tests against your platform are nice-to-have, not required.

This document explains each of those steps in detail.

---

## Why a separate module?

Per `docs/design/multi-engine-design.md` §4.6, each engine / catalog
adapter is its own module. Colocating adapters with engine adapters would
force every consumer of, say, the DuckDB adapter to also take on UC's REST
client dependencies — even if they don't use them. Keeping adapters in
separate modules means `adapters/semanticdf-spark` only depends on Spark,
`adapters/semanticdf-hive-metastore` only depends on HMS Thrift, etc.

**Module layout** (per the existing adapters):

```
adapters/semanticdf-<name>/
├── pom.xml                                  # parent + semanticdf-core dep
├── README.md                                # what this adapter does
└── src/
    ├── main/scala/io/semanticdf/<name>/
    │   ├── <Name>Client.scala                # boundary trait (e.g. HTTP / JDBC / Thrift)
    │   ├── Http<Name>Client.scala            # concrete impl OR
    │   ├── Jdbc<Name>Client.scala             # OR Thrift<Name>Client.scala
    │   ├── <Name>SourceResolver.scala         # extends core SourceResolver
    │   ├── <Name>CatalogAdapter.scala         # extends core CatalogAdapter
    │   ├── <Name>Engine.scala                 # extends core Engine[Any]
    │   └── <Name>ResultEncoder.scala          # maps <Name>Result → PortableQueryResult
    └── test/scala/io/semanticdf/<name>/
        ├── Fake<Name>Client.scala             # hand-driven test fixture
        ├── <Name>SourceResolverSpec.scala
        ├── <Name>CatalogAdapterSpec.scala
        ├── <Name>EngineSpec.scala
        └── <Name>ErrorMappingSpec.scala
```

For the **minimum viable adapter** (Engine + SourceResolver only, no
catalog publish support), you can drop the catalog files and the
catalog adapter test.

---

## Step 1: Copy the skeleton

The [`adapters/semanticdf-template/`](../adapters/semanticdf-template/) directory
contains a working skeleton for a fictional `MyPlatform` adapter. It
includes:

- `MyPlatformClient.scala` — boundary trait (mirror of `UnityCatalogClient`)
- `HttpMyPlatformClient.scala` — JDK `HttpClient` impl (mirror of `HttpUnityCatalogClient`)
- `MyPlatformSourceResolver.scala` — returns `ResolvedSource` sealed ADT
- `MyPlatformCatalogAdapter.scala` — implements all 3 publish modes + CAS
- `MyPlatformEngine.scala` — overrides `executePortable` with a real `ResultEncoder`
- `MyPlatformResultEncoder.scala` — maps `MyPlatformResult` → `PortableQueryResult`
- `MyPlatformError.scala` — sealed ADT (mirror of `HeraClientError`)
- `FakeMyPlatformClient.scala` — test fixture with `def empty`
- `MyPlatform*Spec.scala` — 5 test classes covering the standard's required assertions

Copy the directory:

```bash
cp -r adapters/semanticdf-template/ adapters/semanticdf-myname/
cd adapters/semanticdf-myname/
# Then sed-rename MyPlatform → MyName everywhere
find . -type f -name "*.scala" -exec sed -i 's/MyPlatform/MyName/g' {} +
# Add the new module to parent pom.xml
```

Then register the module in the parent [`pom.xml`](../../../pom.xml):

```xml
<module>adapters/semanticdf-myname</module>
```

---

## Step 2: Implement the boundary trait

**Rule of thumb:** the boundary trait declares **only the methods you
need**. Don't pad it with future-proofing methods you'll "add later" —
per karpathy §2 (simplicity first), write what you need now.

```scala
trait MyNameClient extends Serializable {
  // Per scala-data-driven-refacer §1: pure contract (no behavior).
  // Per error-handling-style.md: public methods return Either[MyNameError, X]
  // where MyNameError is a sealed ADT (NOT a string, NOT a Throwable).
  def executeQuery(sql: String, realmId: String): Either[MyNameError, MyNameResult]
  def describeTable(table: String, realmId: String): Either[MyNameError, Schema]
  // ...etc.
}
```

**The hard bans** from `error-handling-style.md`:

- ❌ `catch { case _: Exception => ... }` — catch specific JDK exception types instead
- ❌ `Either[String, X]` — use the sealed ADT
- ❌ `Either[Throwable, X]` — same
- ❌ Generic `ServerError(String)` — every failure mode deserves its own case
- ❌ `throw new UnsupportedOperationException` at a converter boundary
- ❌ Throwing from an `Either`-returning function

---

## Step 3: Define the typed error ADT

Each adapter has its own `*Error` ADT (mirror `HeraClientError.scala`,
`CatalogError.scala`, `EngineError.scala` in core). Each case is
**specific** — not a generic "something went wrong":

```scala
sealed trait MyNameError extends Product with Serializable
object MyNameError {
  final case class Unauthorized(reason: String) extends MyNameError
  final case class Forbidden(reason: String) extends MyNameError
  final case class NotFound(reason: String) extends MyNameError
  final case class Conflict(reason: String) extends MyNameError
  final case class Network(reason: String) extends MyNameError
  final case class MalformedResponse(reason: String) extends MyNameError
}
```

Per scala-chaos-testing §2 ("silence is a symptom"): a generic
`ServerError(String)` swallows the specific failure mode. Was it a
network blip? a query syntax error? a permissions failure? Each
case matters because callers retry / recover differently.

---

## Step 4: Map your platform's status codes to error cases

At the HTTP / JDBC / Thrift boundary, translate platform-specific
errors to your `MyNameError` cases. **Map specific exceptions
specifically** — never catch-all:

```scala
// GOOD (per error-handling-style.md "Worked example")
try {
  val resp = http.send(req, BodyHandlers.ofString())
  resp.statusCode() match {
    case 200 => Right(resp.body())
    case 401 => Left(MyNameError.Unauthorized(reason = "..."))
    case 403 => Left(MyNameError.Forbidden(reason = "..."))
    case 404 => Left(MyNameError.NotFound(reason = "..."))
    case 409 => Left(MyNameError.Conflict(reason = "..."))
    case _   => Left(MyNameError.MalformedResponse(reason = s"HTTP ${resp.statusCode()}: ${resp.body().take(200)}"))
  }
} catch {
  case _: java.io.IOException       => Left(MyNameError.Network(reason = "network error"))
  case _: InterruptedException      => Left(MyNameError.Network(reason = "timeout"))
  // NEVER: case _: Exception => ...
}

// BAD (catches everything; loses the specific failure mode)
try { ... } catch {
  case e: Exception => Left(MyNameError.Network(reason = s"failed: ${e.getMessage}"))
}
```

---

## Step 5: Implement the three ports

### `SourceResolver` — resolve a `SourceRef` to a `ResolvedSource`

Per the standard: **`ResolvedSource` is a sealed ADT that IS the failure
mode**. You do NOT return `Either[ResolvedSource, X]` — the ADT itself
has 4 cases (`Scan`, `NotFound`, `AuthFailed`, `Incompatible`):

```scala
class MyNameSourceResolver(client: MyNameClient) extends SourceResolver {
  override def resolve(source: SourceRef, identity: EngineIdentity): ResolvedSource =
    source match {
      case SourceRef.ByName(catalog, namespace, table) =>
        client.describeTable(table, realmIdFor(catalog)) match {
          case Right(schema)  => ResolvedSource.Scan(source, schema)
          case Left(MyNameError.NotFound(r)) => ResolvedSource.NotFound(source, r)
          case Left(MyNameError.Unauthorized(r)) => ResolvedSource.AuthFailed(source, r)
          case Left(other) => ResolvedSource.Incompatible(source, s"${other.getClass.getSimpleName}: $r")
        }
      case _: SourceRef.ByPath =>
        ResolvedSource.Incompatible(source, "MyName resolver doesn't support path-based sources")
      case _: SourceRef.ByProvider =>
        ResolvedSource.Incompatible(source, "MyName resolver doesn't support provider-based sources")
    }
}
```

### `CatalogAdapter` — publish + discover + list with CAS

All 3 methods return `Either[CatalogError, X]`. The 3 publish modes each
have a SPECIFIC failure path (NOT throw):

```scala
class MyNameCatalogAdapter(client: MyNameClient, val catalog: String) extends CatalogAdapter {
  override def publish(identity: CatalogIdentity, doc: Any, as: CatalogEntity, mode: PublishMode): Either[CatalogError, PublishResult] = {
    val newDigest = if (doc == null) "doc-placeholder" else doc.toString

    // Per the chaining rule, use for-comprehension for 3+ sequential steps.
    val currentResult: Either[CatalogError, Option[MyNameMeta]] =
      client.getTableMeta(identity.name) match {
        case Right(meta) => Right(Some(meta))
        case Left(MyNameError.NotFound(_)) => Right(None)  // absent → Right(None), NOT error
        case Left(other) => Left(myNameToCatalogError(other))
      }

    currentResult.flatMap { currentMeta =>
      (currentMeta, mode) match {
        case (None, PublishMode.CreateOnly) =>
          createAndReturn(identity, newDigest)  // returns Inserted
        case (None, PublishMode.Upsert) =>
          createAndReturn(identity, newDigest)
        case (None, PublishMode.CompareAndSet(_)) =>
          // Per the CAS contract: nothing at identity → digest can't match → Conflict with no current.
          Right(PublishResult.Conflict(reason = "no entity at identity"))
        case (Some(existing), PublishMode.CreateOnly) =>
          Right(PublishResult.Conflict(reason = "already exists", current = Some(metaToRef(identity, existing))))
        case (Some(existing), PublishMode.Upsert) =>
          commit(identity, newDigest, existing.version).map { newMeta =>
            PublishResult.Updated(metaToRef(identity, existing), metaToRef(identity, newMeta))
          }
        case (Some(existing), PublishMode.CompareAndSet(expectedDigest)) =>
          if (existing.digest == expectedDigest) {
            commit(identity, newDigest, existing.version).map { newMeta =>
              PublishResult.Updated(metaToRef(identity, existing), metaToRef(identity, newMeta))
            }
          } else {
            Right(PublishResult.Conflict(reason = "digest mismatch", current = Some(metaToRef(identity, existing))))
          }
      }
    }
  }
  // discover + list: see the existing implementations (HiveMetastoreCatalogAdapter, UnityCatalogCatalogAdapter)
}
```

### `Engine[R]` — compile + execute + `executePortable`

The **most important override** is `executePortable`. The trait default
throws `NotImplementedError` as a fail-loud sentinel. Your adapter MUST
override this with a real implementation:

```scala
class MyNameEngine(client: MyNameClient, realmId: String) extends Engine[Any] {
  val identity: String = s"myname:$realmId"
  val capabilities: Set[Capability] = Set(/* your supported features */)

  // ... compile(model), compile(plan), execute(...), explain(...)

  override def executePortable(plan: ExecutionPlan[Any], ctx: EngineContext): Either[EngineError, PortableQueryResult] = {
    if (ctx.cancelled) return Left(EngineError.CancellationFailed(reason = "cancelled by caller"))
    val sql = plan.native.asInstanceOf[String]
    for {
      raw <- client.executeQuery(sql, realmId).left.map(myNameToEngineError)
      encoded = MyNameResultEncoder.encode(raw)
    } yield encoded
  }
}
```

---

## Step 6: Write tests using a `Fake<X>Client`

**Always write a hand-driven fake** — never use a mock framework. The fake
is a `Map`-backed data table that returns scripted responses (mirror
`FakeUnityCatalogClient` / `FakeHeraClient`):

```scala
final class FakeMyNameClient(
    initialTables: Map[String, MyNameMeta] = Map.empty,
    initialResults: Map[String, MyNameResult] = Map.empty,
) extends MyNameClient {
  // Mutable state — tests pre-populate, adapter tests mutate.
  private val tables = scala.collection.mutable.Map.from(initialTables)
  private val results = scala.collection.mutable.Map.from(initialResults)

  override def executeQuery(sql: String, realmId: String): Either[MyNameError, MyNameResult] = {
    // Per scala-data-driven-refacer §1: pure data — answer the question
    // deterministically.
    results.get(sql) match {
      case Some(r) => Right(r)
      case None    => Left(MyNameError.NotFound(reason = s"no scripted response for SQL: $sql"))
    }
  }

  override def getTableMeta(table: String, realmId: String): Either[MyNameError, MyNameMeta] = {
    tables.get(table) match {
      case Some(meta) => Right(meta)
      case None => Left(MyNameError.NotFound(reason = s"table '$table' not found"))
    }
  }

  // ... other methods (record calls, mutate store as needed)
}

object FakeMyNameClient {
  // CRITICAL: `def empty` (NOT `val empty`) per the lesson from PR #423.
  // Each test gets a fresh, un-polluted fake.
  def empty: FakeMyNameClient = new FakeMyNameClient()
}
```

**The `def empty` (not `val`) is load-bearing** — the cross-test state
pollution bug in PR #423 caused 6 spurious failures. Use `def`.

---

## Step 7: Tests must cover the standard's required assertions

Every test class should include:

1. **Instance-shape test** — "X is a Y (contract conformance)" — pins that
   you extended the right trait
2. **Happy path** — minimum 2-3 tests covering normal operation
3. **Every publish mode** — CreateOnly (empty + existing), Upsert
   (empty + existing), CompareAndSet (matching + mismatched)
4. **discover** — match + stale + absent
5. **list** — basic + name prefix
6. **Error mapping** — for every `MyNameError` case, assert the
   adapter surfaces it via the typed `Either`

Per `scala-spark-batch-bugs §1`: assert actual behavior (the SQL string,
the optLock value, the entity metadata), not just "the call returned
something."

---

## Step 8: Wire into the MCP registry (optional)

If your adapter should be discoverable via the MCP `list_models` tool,
register it in `semanticdf-mcp/.../Server.scala`. See the existing
registrations for `spark`, `trino`, `duckdb`, and `hera`.

---

## Common pitfalls (from the existing 6 adapters)

1. **Don't write a generic catch-all `Exception` handler** — even when
   the platform throws many exception types. Map each one to a specific
   error case. The fake tests should also exercise these specific cases.

2. **Don't return `Either[String, X]` in a public method** — even
   privately. Use a sealed ADT. Strings lose the type info that the
   compiler needs for exhaustive pattern matching at the call site.

3. **Don't use a `Map[String, String]` for adapter-specific metadata.**
   Use a sealed ADT (mirror `HeraTableMeta`, `UcTableSchema`). Per
   scala-data-driven-refacer §3 ("A rule becomes data only when it
   must change without a deploy"): adapter-specific metadata is
   fixed at compile time, not loaded from config — sealed ADT wins.

4. **Don't use `val empty` for fakes** — `def empty`. This is the lesson
   from PR #423 (`FakeHiveMetastoreClient` cross-test pollution).
   Mutable fakes MUST be re-constructed per test.

5. **Don't bypass `executePortable`** — the trait default throws.
   Every engine adapter MUST override with a real `ResultEncoder`
   implementation.

6. **Don't return `Right(())` for the "entity already exists" case
   in CreateOnly mode** — return `Right(PublishResult.Conflict(...))`
   so callers can distinguish success from collision.

7. **Don't use the `assertThrows` macro for "this should fail loud"**
   — per the standard, "throw at boundary" is deprecated. Return
   `Left(MyNameError.UnsupportedCapability)` instead.

---

## Reference: the existing adapters

Use these as templates for your work:

| Adapter | Ports | LoC (main) | When to mirror |
|---|---|---|---|
| `semanticdf-spark` | Engine (full) | ~3000 | When your engine IS Spark (rare) |
| `semanticdf-trino` | Engine + SourceResolver (full) | ~2000 | When your engine speaks SQL over JDBC |
| `semanticdf-duckdb` | Engine + SourceResolver (full) | ~1300 | When your engine is in-process (smallest engine adapter) |
| `semanticdf-unity-catalog` | SourceResolver + CatalogAdapter | ~800 | When your catalog is REST-based (UC-shaped) |
| `semanticdf-hive-metastore` | SourceResolver + CatalogAdapter | ~750 | When your catalog is Thrift-based (HMS-shaped) |
| `semanticdf-hera` | Engine + SourceResolver + CatalogAdapter (full) | ~2400 | When your platform is REST + has all 3 port needs (full-platform example) |

For a **minimum viable REST adapter**, mirror `semanticdf-hera` and
delete the parts you don't need. For a **minimum viable JDBC adapter**,
mirror `semanticdf-duckdb`.

---

## See also

- [`error-handling-style.md`](../design/error-handling-style.md) — the
  error-handling standard this document is consistent with
- [`multi-engine-design.md`](../design/multi-engine-design.md) — the
  architectural design doc
- [`v0.3.1-feature-parity-backlog.md`](../design/v0.3.1-feature-parity-backlog.md) —
  historical context on why these patterns were chosen
- The [`adapters/semanticdf-template/`](../adapters/semanticdf-template/) skeleton
- The `scala-data-driven-refactor` skill — read it once before writing
  any ADT

---

*Last updated: post-v0.3.1 (Hera adapter landed; engine-portable contract stable).*