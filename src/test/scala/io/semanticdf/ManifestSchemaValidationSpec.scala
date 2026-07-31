package io.semanticdf

import com.fasterxml.jackson.databind.ObjectMapper
import com.networknt.schema.{JsonSchema, JsonSchemaFactory, SchemaValidatorsConfig, SpecVersion}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors
import scala.jdk.CollectionConverters._

/** Production-grade build-time validation of every manifest artifact in
  * the repo against `schemas/manifest.schema.json`.
  *
  * This spec runs as part of `mvn test` (and `mvn verify` in CI), so a
  * malformed example manifest fails the build before it reaches CI
  * and prevents bad manifests from being checked in.
  *
  * Why an integration spec and not a Maven plugin:
  *   - Reuses the existing `mvn test` infrastructure (no new phase to
  *     wire up).
  *   - Same test framework as the rest of the suite; failures are
  *     visible in the same report.
  *   - Network deps (networknt json-schema-validator) are test-scope
  *     and don't bloat the library's runtime classpath.
  *   - For libraries that ship JSON artifacts, the production-grade
  *     pattern is exactly this: schema lives in `schemas/`, the test
  *     suite validates every shipped artifact against it on every
  *     commit. This is what Apache Camel, Spring Cloud Contract, and
  *     many other production libraries do.
  */
class ManifestSchemaValidationSpec extends AnyFunSuite with Matchers {

  private val mapper = new ObjectMapper()

  // The schema is loaded from the test classpath (src/test/resources/
  // manifest.schema.json). To keep a single source of truth, the test
  // resource MUST be byte-identical to schemas/manifest.schema.json at
  // the repo root. The "schema parity" test below enforces this on
  // every `mvn test` run; a hand-edit to one copy that doesn't land in
  // the other fails the build before CI. (Both are committed; a
  // future PR could derive one from the other at build time via a
  // Maven resource-filter / properties step.)
  private val SCHEMA_RESOURCE = "manifest.schema.json"

  // Lazy load: only resolve the schema + scan the repo once per spec class
  // (ScalaTest reuses a single class instance across tests in a run).
  private lazy val schema: JsonSchema = {
    val schemaStream = getClass.getClassLoader
      .getResourceAsStream("manifest.schema.json")
    if (schemaStream == null)
      throw new IllegalStateException(
        "manifest.schema.json (from src/test/resources) not found on the test classpath; " +
        "Maven's standard src/main/resources -> target/classes copy didn't run. " +
        "Check the build lifecycle (the schema is in `schemas/` and copied by the " +
        "Maven resources plugin).")
    val config = SchemaValidatorsConfig.builder().build()
    val factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
    try {
      factory.getSchema(schemaStream, config)
    } finally {
      schemaStream.close()
    }
  }

  // Find the repo root by walking up from the working dir until we see
  // `examples/`. Maven's test phase runs with cwd=project root for the
  // parent module, so the lookup is straightforward.
  private lazy val repoRoot: Path = {
    var cwd = Paths.get("").toAbsolutePath
    while (cwd != null && !Files.isDirectory(cwd.resolve("examples"))) {
      cwd = cwd.getParent
    }
    if (cwd == null)
      throw new IllegalStateException("could not locate repo root from cwd")
    cwd
  }

  private def findManifestFiles(): Seq[Path] = {
    val examples = repoRoot.resolve("examples")
    if (!Files.isDirectory(examples))
      throw new IllegalStateException("examples/ not found at " + examples)
    val stream = Files.walk(examples, 8)
    try {
      stream
        .filter(Files.isRegularFile(_))
        .filter(p => p.getFileName.toString.endsWith(".json"))
        // Skip the v0.1.9/legacy hand-rolled example manifest that
        // intentionally uses an OLDER schemaVersion (pre-1.0-prefix
        // support was added in v0.1.11). We test it separately below.
        .filter(p => !p.endsWith("orders.joined-envelope.json"))
        .filter(p => !p.toString.contains("/target/"))   // skip build output
        // Skip the dbt reader's example manifest — dbt's manifest.json
        // is a dbt-core artifact, not a semanticdf manifest. It has a
        // completely different shape (nodes, sources, parent_map) and
        // is validated separately by the DbtManifestReaderSpec.
        .filter(p => !p.toString.contains("/dbt-reader/"))
        .collect(Collectors.toList[Path])
        .asScala
        .toSeq
    } finally stream.close()
  }

  private def describeViolations(schema: JsonSchema, json: com.fasterxml.jackson.databind.JsonNode): String = {
    val msgs = schema.validate(json).asScala.toList
    if (msgs.isEmpty) "<no messages>"
    else msgs.map(_.getMessage).mkString("\n  - ")
  }

  test("every example manifest JSON validates against schemas/manifest.schema.json") {
    val manifests = findManifestFiles()
    manifests should not be empty
    info("[validator] scanning " + manifests.size + " manifest file(s)")

    manifests.foreach { path =>
      withClue(s"validation failed for ${path}: ") {
        val json = mapper.readTree(Files.readString(path))
        val errors = schema.validate(json)
        if (!errors.isEmpty) {
          fail(s"manifest at $path has schema violations:\n${describeViolations(schema, json)}")
        }
      }
    }
  }

  test("legacy v0.1.9 manifest in joined-manifest-split is intentionally a pre-1.0 example") {
    // The hand-rolled `orders.joined-envelope.json` predates the v0.1.11
    // schema-prefix support and uses the bare `v0.1.11-manifest`
    // schemaVersion (which is fine, since the prefix matches). This
    // test verifies the demo's wire shape round-trips through the
    // v0.1.11+ schema - confirming the hand-rolled example is
    // backward-compatible with the canonical Path-C schema.
    val legacyPath = repoRoot.resolve("examples/joined-manifest-split/target/manifests/orders.joined-envelope.json")
    if (Files.exists(legacyPath)) {
      val json = mapper.readTree(Files.readString(legacyPath))
      val errors = schema.validate(json)
      if (!errors.isEmpty) {
        fail(s"legacy hand-rolled manifest has schema violations:\n${describeViolations(schema, json)}")
      }
    } else {
      info("[validator] legacy hand-rolled manifest not built yet (run `mvn package` in joined-manifest-split first)")
    }
  }

  test("schema documents the runtime.materializeLevel property (regression: post-#314 audit MED-1)") {
    // PR #314 added `materializeLevel` to the writer/reader pair
    // but missed both schema files. The wire format accepts the
    // field (the schema has implicit `additionalProperties: true`),
    // so the existing manifest-validation test passes either way;
    // a regression that removed the field from the schema would
    // only be caught by inspecting the schema itself. Downstream
    // tooling (OKF generators, lineage trackers, MCP introspection)
    // introspects the schema to discover available fields, so the
    // schema MUST document the field for the wire format to be
    // self-describing.
    //
    // Falsification: revert the schema's `materializeLevel` block
    // and this test fails with a clear message naming the missing
    // property. Restore and it passes.
    val schemaTree = schema.getSchemaNode
    val runtime = schemaTree.path("properties").path("runtime")
    runtime.isObject shouldBe true
    val matLevel = runtime.path("properties").path("materializeLevel")
    withClue("schema.properties.runtime.properties.materializeLevel is missing: ") {
      matLevel.isObject shouldBe true
    }
    // The 5-field encoding requires all of these (PR #314 wire format).
    // The post-#315 audit flagged that the test only checked presence,
    // not types or constraints. A regression that flipped `type: boolean`
    // to `type: string` on any field would silently slip through, then
    // bite downstream tools that introspect the schema. Tighten the
    // assertion to check both presence AND shape.
    val required = List("useDisk", "useMemory", "useOffHeap", "deserialized", "replication")
    val expectedTypes = Map(
      "useDisk"      -> "boolean",
      "useMemory"    -> "boolean",
      "useOffHeap"   -> "boolean",
      "deserialized" -> "boolean",
      "replication"  -> "integer",
    )
    required.foreach { f =>
      val prop = matLevel.path("properties").path(f)
      withClue(s"schema.properties.runtime.properties.materializeLevel.properties.$f is missing: ") {
        prop.isObject shouldBe true
      }
      withClue(s"schema.properties.runtime.properties.materializeLevel.properties.$f has wrong type (expected ${expectedTypes(f)}): ") {
        prop.path("type").asText() shouldBe expectedTypes(f)
      }
    }
    // `replication` is `StorageLevel.apply`'s last arg (Int) and is
    // always >= 1 in Spark — `StorageLevel.NONE` has replication = 1,
    // the standard levels go up to 3 (DISK_ONLY_3). A regression that
    // dropped `minimum: 1` would let hand-rolled manifests declare
    // replication = 0, which StorageLevel.apply rejects.
    withClue("schema.properties.runtime.properties.materializeLevel.properties.replication.minimum must be 1: ") {
      matLevel.path("properties").path("replication").path("minimum").asInt(0) shouldBe 1
    }
    // `required` array forces all 5 fields on the writer. The writer
    // always emits all 5 (PR #314 wire format); a hand-rolled manifest
    // missing any field is rejected — matches Spark's StorageLevel
    // semantics (no optional fields).
    val requiredList = matLevel.path("required").asScala.map(_.asText).toSet
    withClue(s"schema.properties.runtime.properties.materializeLevel.required must include all 5 fields: ") {
      requiredList shouldBe required.toSet
    }
  }

  test("schemas/manifest.schema.json (repo root) == src/test/resources/manifest.schema.json (test classpath)") {
    // The two copies exist for different audiences:
    //   1. `schemas/manifest.schema.json` — public artifact shipped
    //      with the repo (visible in source control, linkable from docs).
    //   2. `src/test/resources/manifest.schema.json` — classpath copy
    //      loaded by `getResourceAsStream` for in-process validation.
    // They MUST be byte-identical: the public schema is the same shape
    // as what the library validates against in tests. Drift between
    // them means a hand-edit to one copy didn't land in the other, so
    // the public schema and the test-validated schema disagree — a
    // silent regression for downstream tooling that introspects the
    // public schema to discover fields.
    //
    // Falsification (post-#315 audit MED-1, Architect): the comment on
    // line 35 previously claimed this test existed. It didn't. Inject
    // drift into the repo-root copy only (leave classpath untouched) —
    // the manifest-validation spec still passes (it loads the classpath
    // copy), so drift goes undetected. This test now closes that gap:
    // the parity check fails with a clear message naming the two paths.
    val repoSchemaPath = repoRoot.resolve("schemas/manifest.schema.json")
    val classpathStream = getClass.getClassLoader.getResourceAsStream("manifest.schema.json")
    if (classpathStream == null)
      throw new IllegalStateException(
        "manifest.schema.json (from src/test/resources) not found on the test classpath; " +
        "Maven's standard src/test/resources -> target/test-classes copy didn't run. " +
        "Check the build lifecycle (the schema is in `schemas/` and copied by the " +
        "Maven resources plugin).")
    val (repoTree, classpathTree) =
      try {
        val repo      = mapper.readTree(Files.readString(repoSchemaPath))
        val classpath = mapper.readTree(classpathStream)
        (repo, classpath)
      } finally classpathStream.close()
    withClue(s"drift between $repoSchemaPath and src/test/resources/manifest.schema.json: ") {
      repoTree shouldBe classpathTree
    }
  }
}
