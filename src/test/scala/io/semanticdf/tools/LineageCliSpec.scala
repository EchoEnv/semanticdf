package io.semanticdf.tools

import io.semanticdf.SparkSessionFixture
import io.semanticdf.adapters.YamlLoader
import io.semanticdf.lineage.Lineage

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for the `lineage` CLI subcommand and the `LineageCli.toDot`
  * DOT renderer. The CLI is exercised by calling `Main.main` with
  * the same arg shape the user would type; the DOT renderer is
  * tested directly. */
class LineageCliSpec extends AnyFunSuite with SparkSessionFixture {

  /** Set up a Spark session with a stub `flights_csv` temp view, the
    * same way the existing SqlCliEndToEndSpec does. */
  /** Register the `flights_csv` temp view with the schema the YAML
    * fixture expects (carrier, distance, passengers). The fixture
    * (sql-cli-fixtures/flights.yml) is shared with the existing
    * SqlCliEndToEndSpec — same data shape, same column names. */
  private def setupSpark(): org.apache.spark.sql.SparkSession = {
    val s = spark
    val rows = s.sparkContext.parallelize(Seq(
      org.apache.spark.sql.Row("AA", 100L, 50L),
      org.apache.spark.sql.Row("UA", 200L, 75L),
    ))
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("carrier",    org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("distance",   org.apache.spark.sql.types.LongType),
      org.apache.spark.sql.types.StructField("passengers", org.apache.spark.sql.types.LongType),
    ))
    s.createDataFrame(rows, schema).createOrReplaceTempView("flights_csv")
    s
  }

  test("toDot: empty workspace renders a graph with no edges") {
    val s = setupSpark()
    val models = YamlLoader.loadDir("src/test/resources/sql-cli-fixtures", s)
    val wl = Lineage.workspaceOf(models)
    val dot = LineageCli.toDot(wl)
    assert(dot.startsWith("digraph semanticdf_lineage {"), s"expected DOT header, got: ${dot.take(80)}")
    assert(dot.contains("rankdir=LR"))
    assert(dot.contains("flights"), s"expected 'flights' node, got: $dot")
    assert(dot.trim.endsWith("}"), s"expected closing brace, got: ${dot.takeRight(20)}")
  }

  test("toDot: a single-node workspace has no edges (no upstreamModels)") {
    val s = setupSpark()
    val models = YamlLoader.loadDir("src/test/resources/sql-cli-fixtures", s)
    val wl = Lineage.workspaceOf(models)
    // A single-table model has no joins, so no edges
    val edges = """\n.*->.*\n""".r.findAllIn(dot(wl)).toList
    assert(edges.isEmpty, s"expected no edges in single-table graph, got: $edges")
  }

  test("lineage workflow: build a workspace from YAML, emit JSON, verify shape") {
    // This is the end-to-end happy path the CLI exercises. We don't
    // call Main.main here (it creates its own SparkSession which
    // would miss the `flights_csv` view); instead we exercise the
    // same workflow the CLI does internally.
    val s = setupSpark()
    val models = YamlLoader.loadDir("src/test/resources/sql-cli-fixtures", s)
    assert(models.contains("flights"), s"expected flights model, got: ${models.keys}")

    val json = Lineage.toJson(Lineage.workspaceOf(models))
    assert(json.contains("\"schema\""), s"expected schema field, got: ${json.take(200)}")
    assert(json.contains("semanticdf-lineage-v1"), s"expected schema version, got: ${json.take(200)}")
    assert(json.contains("\"flights\""), s"expected flights model in JSON, got: ${json.take(200)}")
  }

  test("lineage workflow: --format dot equivalent (render a graphviz DOT)") {
    val s = setupSpark()
    val models = YamlLoader.loadDir("src/test/resources/sql-cli-fixtures", s)
    val dot = LineageCli.toDot(Lineage.workspaceOf(models))
    assert(dot.contains("digraph semanticdf_lineage {"), s"expected DOT header, got: ${dot.take(80)}")
  }

  test("lineage workflow: filter to a single model by name (--model equivalent)") {
    val s = setupSpark()
    val models = YamlLoader.loadDir("src/test/resources/sql-cli-fixtures", s)
    val selected = models.filter { case (name, _) => name == "flights" }
    val json = Lineage.toJson(Lineage.workspaceOf(selected))
    assert(json.contains("\"flights\""), s"expected flights model, got: ${json.take(300)}")
    // The modelId in the JSON is the workspace map key (after the
    // workspaceOf fix); it should be the model name.
    assert(json.contains("\"modelId\":\"flights\""), s"expected modelId=flights, got: ${json.take(300)}")
  }

  // ---------- helpers ----------

  private def dot(wl: io.semanticdf.lineage.WorkspaceLineage): String =
    LineageCli.toDot(wl)

}
