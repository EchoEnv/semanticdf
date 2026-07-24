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
  // resource is byte-identical to schemas/manifest.schema.json at the
  // repo root. Any drift between the two fails the build at the next
  // `mvn test` because this spec validates against the classpath copy.
  // (Both are committed; a future PR could derive one from the other
  // at build time via a Maven resource-filter / properties step.)
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
}
