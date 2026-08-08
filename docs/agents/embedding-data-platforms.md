# Embedding Data Platforms — semanticdf

**Status:** v1 — current. Audience: anyone integrating semanticdf
with a data platform that requires in-process transport (JDBC / Thrift /
gRPC / in-memory REST) rather than HTTP-from-the-driver.

**The TL;DR:**

1. For **JDBC** (Spark, Trino, DuckDB), the existing pattern is
   `local[2]` Spark + per-test H2/Derby. See [§JDBC](#jdbc).
2. For **Thrift** (HMS, Databricks Connect), the existing pattern is
   the Apache Hive `HiveMetaStoreClient` with a remote URI; in-process
   mode is provided by Apache Hive's embedded HSQLDB backend. See
   [§Thrift](#thrift).
3. For **gRPC** (Spark Connect), the existing pattern is the
   `org.apache.spark.sql.SparkSession.Builder.remote(url)` API. See
   [§gRPC](#grpc).
4. For **in-process REST** (e.g. embedding a stub server for testing
   your adapter without a live platform), the pattern is
   `com.sun.net.httpserver.HttpServer` from the JDK stdlib. See
   [§In-process REST](#in-process-rest).

This document explains each pattern with a working example + the
gotchas.

---

## Why embed?

Most semanticdf adapters talk to a remote data platform over HTTP /
Thrift / gRPC. That works for production (real Trino, real Hera, real
HMS) and for integration tests (a real instance running in CI or
locally). But sometimes you want:

- **Unit tests** that run without a real platform instance
  (CI builds, developer laptops, sandboxed environments).
- **Local development** when the remote platform is unavailable
  (offline, network-blocked, on a different VPC).
- **Reproducible benchmark runs** without a flaky remote dependency.

Embedding gives you all three.

Per the user's standing rule: "use the same API as the original Spark
library" — we don't invent new abstractions for embedding; we use the
platform's own in-process mode when one exists.

---

## JDBC

### When to use

Your adapter connects to a SQL warehouse via JDBC (Trino, DuckDB,
Postgres, MySQL, H2, Derby, Snowflake via JDBC, BigQuery via JDBC,
etc.). You want to run a test without a real database server.

### Pattern: per-test H2 / Derby

The Spark adapter's `SparkSessionFixture` already does the
in-memory-Spark equivalent. For a per-test JDBC connection, the
pattern is:

```scala
import java.sql.DriverManager

class MyJdbcAdapterSpec extends AnyFunSuite with Matchers {

  /** Spin up a fresh in-memory H2 instance per test.
    *
    * Per the standard's "may not exist" rule + internal helper rule
    * (ONE call site, caller does `match` immediately → plain function),
    * this returns a `Connection` (not `Either[L, Connection]`). */
  private def newConnection(): Connection = {
    DriverManager.getConnection(
      "jdbc:h2:mem:test_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1",
      "sa",  // H2 default user
      "",    // H2 default password (empty)
    )
  }

  test("describe reads the H2 schema") {
    val conn = newConnection()
    try {
      val stmt = conn.createStatement()
      stmt.execute("CREATE TABLE orders (id INT, region VARCHAR(50))")
      stmt.close()

      // Your adapter
      val adapter = new MyJdbcAdapter(conn)
      val schema = adapter.describeTable("orders")
      // assert schema is ResolvedSchema(Map("id" -> "int", "region" -> "varchar"))
    } finally {
      // Per error-handling-style.md: catch specific exceptions
      // (no catch-all `Exception`). Connection.close() throws
      // SQLException, which we surface explicitly.
      try conn.close() catch { case _: java.sql.SQLException => () }
    }
  }
}
```

### Gotchas

- **`DB_CLOSE_DELAY=-1`** keeps the in-memory database alive between
  connections within the same JVM. Without it, H2 deletes the database
  on the last `close()` and a subsequent test that reuses the same
  URL gets a fresh empty DB.
- **`System.nanoTime()` suffix** in the URL: H2 in-memory DBs are
  per-URL. Using the same URL across tests causes test pollution.
  Append a unique suffix per test (or use `ThreadLocal` cleanup, but
  per-test instances are simpler).
- **H2 quirks** vs real Postgres / MySQL: H2 supports most SQL but
  has different escaping, identifier quoting, and reserved words.
  Use a test-only `dialect` for the `MyJdbcAdapter`'s code path,
  then run integration tests against the real warehouse separately.
- **Don't use `try-finally` for cleanup** if your production code path
  uses `Either` — per the standard, exceptions in cleanup paths should
  be caught specifically, not silently swallowed.

### Existing examples in the codebase

- `adapters/semanticdf-spark/src/test/scala/io/semanticdf/SparkSessionFixture.scala`
  — in-process Spark (the "local[2]" pattern, not JDBC, but the same
  fixture-trait shape)
- `adapters/semanticdf-trino/.../trino/JdbcTrinoConnection.scala` — the
  production-side Trino JDBC connection (use this as your reference
  for what a JDBC client trait should look like)

---

## Thrift

### When to use

Your adapter connects to a service over Apache Thrift (HMS,
Databricks Connect's older API, ScyllaDB, some Cassandra drivers).
You want to run a test without a real Thrift server.

### Pattern: use Apache Hive's embedded HSQLDB backend

The Apache Hive project ships `org.apache.hive:hive-metastore` which
includes a built-in "embedded" mode backed by HSQLDB. The existing
Hera `ThriftHiveMetastoreClient` uses the **remote** mode
(`hive-metastore.uris=thrift://...`); for tests, you can swap to
embedded:

```scala
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

class HmsAdapterSpec extends AnyFunSuite with Matchers {

  /** In-process HMS client backed by HSQLDB.
    *
    * Per error-handling-style.md: program errors at the boundary
    * (empty conf) throw `IllegalArgumentException`; runtime errors
    * (HMS can't init) return `Left(HmsClientError)`. */
  def newEmbeddedClient(): Either[HmsClientError, HiveMetaStoreClient] = {
    val conf = new HiveConf()
    // Apache Hive reads these to decide embedded mode.
    conf.set("hive.metastore.uris", "")  // empty = embedded
    conf.set("datanucleus.schema.autoCreateAll", "true")
    conf.set("hive.conf.hidden", "true")  // avoid /etc/hive/conf lookup
    try {
      Right(new HiveMetaStoreClient(conf))
    } catch {
      case e: org.apache.hadoop.hive.metastore.api.MetaException =>
        Left(HmsClientError.NetworkError(reason = s"embedded HMS init: ${e.getMessage}"))
    }
  }
  // ... tests
}
```

### Gotchas

- **Embedded HMS uses Derby by default** (not HSQLDB — that's the
  documentation lie). Derby has its own quirks; read [the Hive docs]
  (https://cwiki.apache.org/confluence/display/Hive/AdminManual+MetastoreAdmin)
  before relying on it.
- **`hive.conf.hidden=true`** is critical — without it, the HMS client
  reads `/etc/hive/conf` on Linux, which is slow + has different
  defaults than your test expects. The `ThriftHiveMetastoreClient`
  already sets this; you just need to remember when spinning up
  embedded.
- **The embedded mode is single-JVM only.** Don't try to share it
  across test classes; the lock manager can't handle it.

### Existing examples in the codebase

- `adapters/semanticdf-hive-metastore/src/main/scala/io/semanticdf/hivemetastore/ThriftHiveMetastoreClient.scala`
  — the production-side HMS client (uses REMOTE mode; look here for
  the trait shape + serialization boundary handling)

---

## gRPC

### When to use

Your adapter connects to a service over gRPC (Spark Connect,
Databricks Connect's newer API, some managed-service backends). You
want to test without a real gRPC server.

### Pattern: in-process server via `io.grpc:in-process`

The gRPC ecosystem ships a `InProcessServerBuilder` that's purpose-built
for testing:

```scala
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Server, ManagedChannel}

class SparkConnectAdapterSpec extends AnyFunSuite with Matchers {

  private var server: Server = _
  private var channel: ManagedChannel = _

  override def beforeAll(): Unit = {
    val serverName = InProcessServerBuilder.generateName()
    server = InProcessServerBuilder.forName(serverName)
      .addService(new MySparkConnectServiceImpl(...))  // your mock service
      .build()
      .start()
    channel = InProcessChannelBuilder.forName(serverName)
      .usePlaintext()
      .build()
  }

  override def afterAll(): Unit = {
    if (channel != null) channel.shutdownNow()
    if (server != null) server.shutdownNow()
  }

  test("query returns rows") {
    val stub = MyServiceGrpc.newStub(channel)
    // ... use the stub against your adapter
  }
}
```

### Gotchas

- **Generate a unique server name per test** (or per `beforeAll`).
  Reusing a name across tests causes "server already running" errors.
- **Always `shutdownNow()` the channel and server** — leaks native
  threads. Use `try-finally` with specific exception catches (per
  the standard, not a catch-all `Exception`).
- **Use `usePlaintext()`** — TLS in tests is unnecessary friction.
- **Generate test data deterministically.** A reproducible seed
  (e.g. `new Random(42)`) makes flaky tests debuggable.

### Existing examples in the codebase

- **No existing examples yet.** Spark Connect is used in production
  but not tested in-process. This is a v0.4.0 follow-up.

---

## In-process REST

### When to use

Your adapter talks to a platform over REST. You want to test the
request/response cycle without a real HTTP server (e.g. the platform
requires auth, has rate limits, isn't available in CI). The JDK's
`com.sun.net.httpserver.HttpServer` (built-in, no new dep) is the
canonical choice for a lightweight in-process server.

### Pattern: `com.sun.net.httpserver.HttpServer`

```scala
import com.sun.net.httpserver.{HttpServer, HttpHandler, HttpExchange}
import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

class MyRestAdapterSpec extends AnyFunSuite with Matchers {

  private var server: HttpServer = _
  private var port: Int      = 0

  override def beforeAll(): Unit = {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    port = server.getAddress.getPort
    server.createContext("/api/query", new QueryHandler())
    server.createContext("/api/tables/", new TablesHandler())
    server.start()
  }

  override def afterAll(): Unit = {
    if (server != null) server.stop(0)
  }

  // Handlers record requests + return scripted responses.
  private val scriptedResponses: scala.collection.mutable.Map[String, String] =
    scala.collection.mutable.Map.empty
  private val recordedRequests: scala.collection.mutable.ListBuffer[(String, String)] =
    scala.collection.mutable.ListBuffer.empty

  private class QueryHandler extends HttpHandler {
    override def handle(exch: HttpExchange): Unit = {
      val body = new String(exch.getRequestBody.readAllBytes(), "UTF-8")
      recordedRequests += (("POST", body))
      val responseBody = scriptedResponses.getOrElse("query", "{}")
      exch.sendResponseHeaders(200, responseBody.length.toLong)
      exch.getResponseBody.write(responseBody.getBytes("UTF-8"))
      exch.close()
    }
  }
  // ... similar for TablesHandler
}
```

### Gotchas

- **`new InetSocketAddress("127.0.0.1", 0)`** binds to a random port
  — important for CI where other tests may hold ports. The bound
  port is `server.getAddress.getPort`.
- **Always call `server.stop(0)`** (the `0` means "no delay"). Without
  it, the server keeps accepting requests for `stopDelay` seconds
  (default 5), which slows test teardown.
- **Read the request body in `handle`** — `exch.getRequestBody` is
  a one-shot stream. If you don't read it, the next request hangs.
- **Set `Content-Length` correctly.** `sendResponseHeaders(code, length)`
  uses `length` for the response body size. Use `-1L` for
  chunked-encoding if you don't know the size up-front.
- **Per error-handling-style.md:** catch `IOException` specifically
  in your `HttpHandler` (the handler runs on a JDK thread pool, and
  unhandled `IOException`s leak).

### Existing examples in the codebase

- **No `com.sun.net.httpserver.HttpServer` examples** — Hera and
  UC each use the JDK `java.net.http.HttpClient` but don't have a
  server-side stub for testing. A v0.4.0 follow-up could add one
  (would benefit the `adapters/semanticdf-hera` integration tests
  if we add them in CI without requiring a live Hera instance).

---

## Cross-cutting: the "fake client vs. in-process server" tradeoff

Both fake clients (like `FakeUnityCatalogClient`, `FakeHeraClient`,
`FakeMyPlatformClient`) and in-process servers (H2, embedded HMS,
`HttpServer`, gRPC in-process) serve the same purpose: test the
adapter without a real platform instance. **Which to use?**

| | Fake client | In-process server |
|---|---|---|
| **Setup cost** | Low (~50 LoC per adapter) | Medium (~100-200 LoC per pattern) |
| **Realism** | Tests adapter logic; NOT the wire format | Tests BOTH adapter logic AND wire format |
| **Flakiness** | Zero (no IO) | Low-medium (real IO, but local) |
| **Speed** | <1ms per test | 10-100ms per test |
| **Maintenance** | Adapter author owns the fake | Shared (any HTTP/gRPC test reuses the stub) |
| **Use when** | Unit tests of adapter behavior | Wire-format tests (does the adapter handle 401 vs 403 correctly? does it parse the response JSON correctly?) |

**Recommendation (per the existing pattern across UC/HMS/Hera):** use
fake clients for unit tests, in-process servers for integration tests.
Both belong in the same module.

The new adapter template at `adapters/semanticdf-template/` provides
a `FakeMyPlatformClient` — when the next adapter that needs a real
wire-format test lands, that's the trigger to add the corresponding
in-process stub.

---

## Cross-cutting: per the error-handling standard

All embed patterns above use these standard rules:

- **Programmer errors at the boundary** (empty config, missing
  fields) → `throw IllegalArgumentException` (NOT `Either`).
- **Runtime errors** (H2 can't init, embedded HMS Derby hangs,
  `HttpServer` port already in use) → typed `Either[L, X]` with
  the adapter's sealed `*Error` ADT.
- **IO-boundary catches** → specific exception types
  (`IOException`, `InterruptedException`, `SQLException`), not
  catch-all `Exception`.
- **Cleanup in `afterAll` / `finally`** → use `try-finally` with
  specific catches. Don't let cleanup exceptions mask test results.

---

## See also

- [`adding-a-new-adapter.md`](adding-a-new-adapter.md) — the
  adapter authoring guide (uses fake clients, not in-process servers)
- [`error-handling-style.md`](../design/error-handling-style.md) — the
  error-handling standard referenced throughout this document
- The existing `SparkSessionFixture` trait at
  `adapters/semanticdf-spark/src/test/scala/io/semanticdf/SparkSessionFixture.scala`
  — the canonical example of an in-process test fixture in the
  codebase
- The existing fake clients at
  `adapters/semanticdf-unity-catalog/src/test/.../FakeUnityCatalogClient.scala`,
  `adapters/semanticdf-hive-metastore/src/test/.../FakeHiveMetastoreClient.scala`,
  `adapters/semanticdf-hera/src/test/.../FakeHeraClient.scala` —
  the canonical examples of fake-client patterns

---

*Last updated: post-v0.4.0 Direction A (adapter template). Captures patterns
already in use; new in-process server examples are v0.4.0+ follow-ups.*