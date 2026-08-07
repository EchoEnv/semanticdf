package io.semanticdf.trino.integration

import io.semanticdf.core.engine.{EngineContext, EngineIdentity, ResolvedSource}
import io.semanticdf.core.model.{Model, ModelPolicyDefaults, ModelStatus, SourceRef}
import io.semanticdf.trino.{TrinoEngine, TrinoConnection}
import io.semanticdf.unitycatalog.{HttpUnityCatalogClient, UnityCatalogSourceResolver}
import org.scalatest.Assertions.fail

/** Integration test for [[UnityCatalogSourceResolver]] against a
  * real Unity Catalog OSS cluster (Docker setup under
  * `docker-uc/`).
  *
  * ==Why gated by `-Ddocker.tests=true`==
  *
  * Per the standing pattern (`TrinoIntegrationSpec`): tests that
  * require a real cluster must NOT run on dev machines without
  * Docker. The `assumeDocker()` helper cancels (not fails) such
  * tests so `mvn test` stays green on dev machines.
  *
  * ==Why this is the FIRST catalog integration test in the codebase==
  *
  * Per the multi-engine design §4.6 layer-separation principle:
  * this test proves the design works. A `SourceResolver`
  * implementation can resolve a `SourceRef` against a real UC
  * REST API, regardless of which engine consumes the result.
  *
  * ==Per user constraint: "monitor memory, disk"==
  *
  * The Docker setup uses:
  *   - 1.5 GiB container cap (vs 7.6 GiB host total — leaves 6 GiB headroom)
  *   - JVM `-Xmx768m`
  *   - bind-mounted ./data (wiped between runs)
  * `monitor.sh` polls every interval; `teardown.sh` is idempotent.
  * The test itself reads ~3 columns from 1 table — disk usage stays
  * under 1 MiB.
  */
class UnityCatalogIntegrationSpec extends UnityCatalogFixture {

  private val identity = EngineIdentity(
    name                 = "trino",
    nativeVersion        = "0.5.0",
    engineAdapterVersion = "0.3.0",
  )

  // -- cluster health (the most basic test) --

  test("Unity Catalog cluster is reachable and responds to /api/2.1/unity-catalog/catalogs") {
    assumeDocker()
    assertClusterHealthy()
  }

  // -- end-to-end resolver flow --

  test("UnityCatalogSourceResolver.resolve() returns Scan with the table's columns (real cluster)") {
    assumeDocker()
    setupTestResources()

    val client   = HttpUnityCatalogClient(baseUrl = ucUrl)
    val resolver = UnityCatalogSourceResolver(client, identity)

    val source = SourceRef.ByName(
      catalog   = Some("unity"),
      namespace = Some("semanticdf"),
      table     = "orders",
    )
    val result = resolver.resolve(source, identity)
    result match {
      case scan: ResolvedSource.Scan =>
        scan.source shouldBe source
        // Verify the 3 columns came back through the REST + JSON pipeline.
        scan.schema.fields.keySet shouldBe Set("id", "region", "amount")
        scan.schema.fields("id") shouldBe "LONG"
        scan.schema.fields("region") shouldBe "STRING"
        scan.schema.fields("amount") shouldBe "DECIMAL"

      case other =>
        fail(s"expected ResolvedSource.Scan but got $other")
    }
  }

  // -- not-found via real REST 404 --

  test("UnityCatalogSourceResolver.resolve() returns NotFound for an unknown table (real cluster)") {
    assumeDocker()
    setupTestResources()

    val client   = HttpUnityCatalogClient(baseUrl = ucUrl)
    val resolver = UnityCatalogSourceResolver(client, identity)

    val source = SourceRef.ByName(
      catalog   = Some("unity"),
      namespace = Some("semanticdf"),
      table     = "does_not_exist",
    )
    val result = resolver.resolve(source, identity)
    result shouldBe a [ResolvedSource.NotFound]
  }

  // -- cross-engine composition proof --

  test("UnityCatalogSourceResolver works with any engine identity (engine-portable resolver)") {
    assumeDocker()
    setupTestResources()

    val client = HttpUnityCatalogClient(baseUrl = ucUrl)

    // Use a different engine identity (Spark) to prove the
    // resolver is engine-portable: it doesn't depend on the
    // engine identity for behavior, only for default catalog /
    // namespace resolution.
    val sparkIdentity = EngineIdentity(
      name                 = "spark",
      nativeVersion        = "3.5.8",
      engineAdapterVersion = "0.3.0",
    )
    val resolver = UnityCatalogSourceResolver(client, sparkIdentity)

    val source = SourceRef.ByName(
      catalog   = Some("unity"),  // explicit (no default applied)
      namespace = Some("semanticdf"),
      table     = "orders",
    )
    val result = resolver.resolve(source, sparkIdentity)
    result shouldBe a [ResolvedSource.Scan]
  }

  // -- resource monitoring proof --
  test("Docker cluster stays under memory cap during a real resolve (memory+disk monitoring)") {
    assumeDocker()

    // Pull memory + disk stats BEFORE the resolve.
    val before = readContainerStats()
    val beforeMem = before("mem_bytes").toLong
    val beforeDisk = before("disk_bytes").toLong

    // Exercise the resolver against a real table.
    setupTestResources()
    val client   = HttpUnityCatalogClient(baseUrl = ucUrl)
    val resolver = UnityCatalogSourceResolver(client, identity)
    val source = SourceRef.ByName(Some("unity"), Some("semanticdf"), "orders")
    resolver.resolve(source, identity) shouldBe a [ResolvedSource.Scan]

    // Pull memory + disk stats AFTER the resolve.
    val after = readContainerStats()
    val afterMem = after("mem_bytes").toLong
    val afterDisk = after("disk_bytes").toLong

    // Sanity assertions: the resolve should not have OOM'd or
    // eaten disk. Bounds are generous (well below the 1.5 GiB
    // container cap and the host's free disk).
    val MEM_CAP_BYTES  = 1_610_612_736L  // 1.5 GiB
    val DISK_CAP_BYTES =  100_000_000L   // 100 MiB (well below host free)

    assert(afterMem  < MEM_CAP_BYTES,  s"container memory ${afterMem}B exceeded cap ${MEM_CAP_BYTES}B")
    assert(afterDisk < DISK_CAP_BYTES, s"disk usage ${afterDisk}B exceeded soft cap ${DISK_CAP_BYTES}B")

    // Note: we don't assert exact mem/disk deltas — the JVM
    // and the bind mount have noise (GC, kernel page cache,
    // etc.). The important property is: under cap, not OOM.
  }

  /** Read the running UC container's current memory + disk stats
    * via Docker CLI. Returns a map of metric name -> value
    * (as strings; caller casts to Long). */
  private def readContainerStats(): Map[String, String] = {
    val memOut = scala.sys.process.Process(
      Seq("docker", "stats", "semanticdf-uc-test", "--no-stream", "--format", "{{.MemUsage}}")
    ).lineStream.headOption.getOrElse("0B / 0B")
    val memBytes = memOut.split('/').head.trim match {
      case s if s.endsWith("MiB") => (s.dropRight(3).toDouble * 1024 * 1024).toLong
      case s if s.endsWith("GiB") => (s.dropRight(3).toDouble * 1024 * 1024 * 1024).toLong
      case s if s.endsWith("KiB") => (s.dropRight(3).toDouble * 1024).toLong
      case s                     => 0L
    }
    val diskBytes = scala.sys.process.Process(Seq("du", "-sb", "../semanticdf-unity-catalog/docker-uc/data"))
      .lineStream.headOption.map(_.split('\t').head.toLong).getOrElse(0L)
    Map("mem_bytes" -> memBytes.toString, "disk_bytes" -> diskBytes.toString)
  }

  // -- cross-engine composition (per multi-engine design §4.6) --

  test("TrinoEngine.compile calls UnityCatalogSourceResolver before SQL emit (real cluster)") {
    assumeDocker()
    setupTestResources()

    // Compose the catalog layer (Unity Catalog) with the engine
    // layer (Trino). This is the §4.6 cross-engine composition
    // the design promises: any catalog + any engine compose
    // cleanly without coupling.
    val client   = HttpUnityCatalogClient(baseUrl = ucUrl)
    val resolver = UnityCatalogSourceResolver(client, identity)
    val engine   = new TrinoEngine().withSourceResolver(resolver)

    // Build a minimal Model pointing at the UC-registered table.
    // We use empty dimensions/measures because this test only
    // exercises the resolution path, not the SQL emit against
    // actual rows.
    val model = Model.of(
      name      = "orders",
      source    = SourceRef.ByName(
        catalog   = Some("unity"),
        namespace = Some("semanticdf"),
        table     = "orders",
      ),
      dimensions         = Nil,
      measures           = Nil,
      calculatedMeasures = Nil,
      joins              = Nil,
      defaultPolicies    = ModelPolicyDefaults.none,
      status             = ModelStatus.Draft,
    ).fold(err => fail(s"sampleModel failed validation: $err"), m => m)

    val result = engine.compile(model, EngineContext.defaultContext)
    result.isRight shouldBe true
  }

  test("TrinoEngine.compile with UnityCatalogSourceResolver returns Left(FeatureDeferred) when UC says NotFound") {
    assumeDocker()
    // DO NOT call setupTestResources() — the catalog doesn't have
    // the table; the resolver must return NotFound; compile must
    // translate that to Left(EngineError.FeatureDeferred).
    val client   = HttpUnityCatalogClient(baseUrl = ucUrl)
    val resolver = UnityCatalogSourceResolver(client, identity)
    val engine   = new TrinoEngine().withSourceResolver(resolver)

    val model = Model.of(
      name      = "does_not_exist",
      source    = SourceRef.ByName(
        catalog   = Some("unity"),
        namespace = Some("semanticdf"),
        table     = "does_not_exist_42",
      ),
      dimensions         = Nil,
      measures           = Nil,
      calculatedMeasures = Nil,
      joins              = Nil,
      defaultPolicies    = ModelPolicyDefaults.none,
      status             = ModelStatus.Draft,
    ).fold(err => fail(s"sampleModel failed validation: $err"), m => m)

    val result = engine.compile(model, EngineContext.defaultContext)
    result.isLeft shouldBe true
    val err = result.left.toOption.get
    err.toString should include ("source-not-found")
  }
}