package com.example.manifesttransformsload

import io.semanticdf._
import io.semanticdf.SemanticManifest
import io.semanticdf.SemanticManifest.Identity

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/** Worked example: round-trip a model with transforms through the manifest.
  *
  * Demonstrates:
  *   1. Build a `SemanticTable` programmatically with `withTransforms`
  *      and `withMeasures`. Transforms declare a source expression
  *      string via `exprString = Some(...)` so the writer can serialize
  *      them losslessly.
  *   2. Emit a `SemanticManifest` JSON via `toJson(model, identity)`.
  *      The `transforms[]` block carries each transform's name and
  *      source expression.
  *   3. Re-load the JSON via `SemanticManifest.fromJson` and run queries.
  *      The reader rebuilds the `SemanticTransformsOp` from the
  *      serialized expressions; downstream behavior matches.
  *
  * Run:
  *   1. `mvn install` the parent semanticdf project
  *   2. From this directory: `mvn scala:run -DmainClass=com.example.manifesttransformsload.Main`
  *
  * What this is NOT: it does NOT emit a YAML-and-then-run flow. The
  * transforms are declared programmatically here so the demo is
  * self-contained. A real pipeline would author transforms in YAML
  * (`transforms:` block), run `tools.Main manifest --yaml models/...yml
  * --id io.acme.X --out manifests/X.json` to emit the JSON, and
  * downstream consumers would load the JSON via `fromJson`. */
object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("manifest-transforms-load")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("=== manifest-transforms-load demo ===")

    // ------------------------------------------------------------------
    // 1. Build a source DataFrame with concrete columns.
    // ------------------------------------------------------------------
    val rows = (1 to 5).map { i =>
      Row(i, s"item_$i", i * 10)
    }
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      StructType(Seq(
        StructField("id",    IntegerType),
        StructField("label", StringType),
        StructField("price", IntegerType),
      ))
    )

    // ------------------------------------------------------------------
    // 2. Build a SemanticTable with transforms + measures.
    //
    // The KEY bit: each Transform has `exprString = Some(...)` so the
    // manifest writer serializes the source string instead of the
    // `<lambda>` sentinel. Same pattern applies to Dimension / Measure.
    // ------------------------------------------------------------------
    val st = toSemanticTable(sourceDf, name = Some("items"))
      .withDimensions(
        Dimension("id",    _ => col("id")),
        Dimension("label", _ => col("label")),
      )
      .withTransforms(
        Transform("price_with_tax",
                  _ => expr("price * 1.10"),
                  exprString = Some("price * 1.10")),
      )
      .withMeasures(
        Measure("avg_price",
                _ => expr("avg(price)"),
                exprString = Some("avg(price)")),
        Measure("revenue",
                _ => expr("sum(price_with_tax)"),
                exprString = Some("sum(price_with_tax)")),
      )

    // ------------------------------------------------------------------
    // 3. Emit the manifest with identity fields.
    // ------------------------------------------------------------------
    val identity = Identity(
      id              = "io.example.manifesttransformsload.items",
      manifestVersion = SemanticManifest.InitialManifestVersion,
      namespace       = "demo",
      metadata        = Map("owner" -> "data-platform", "demo" -> "transforms"),
    )
    val json = SemanticManifest.toJson(st, identity, prettyPrint = true)

    // Print the manifest for inspection.
    println("[demo] --- manifest ---")
    println(json)
    println("[demo] --- end manifest ---")

    // Sanity: the JSON has the `transforms[]` block + identity fields.
    val tree = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json)
    assert(tree.has("transforms") && tree.get("transforms").size == 1)
    assert(tree.get("id").asText() == "io.example.manifesttransformsload.items")

    // ------------------------------------------------------------------
    // 4. Re-load the manifest and run a query against the rebuilt model.
    //
    // The reader reconstructs the SemanticTransformsOp. The transform
    // column `price_with_tax` appears in the restored DataFrame; the
    // measure `revenue` (which references `price_with_tax`) computes
    // correctly end-to-end.
    // ------------------------------------------------------------------
    val restored = SemanticManifest.fromJson(json, sourceDf)
    val restoredDf = restored.execute(spark)
    println("[demo] restored columns: " + restoredDf.columns.mkString(", "))

    // The first transform is `price_with_tax` = price * 1.10. Run a
    // query that uses both an original column and a transform output.
    val q = restoredDf
      .select(col("label"), col("price"), col("price_with_tax"))
      .orderBy(col("id"))
    println("[demo] --- query result ---")
    q.show(truncate = false)

    // Group-by + measure query (uses the calc-style wiring but applied
    // to a base measure here for simplicity).
    val agg = restored.groupBy("label").aggregate("avg_price")
    println("[demo] --- aggregate ---")
    agg.execute(spark).show(truncate = false)

    println("=== demo complete ===")
    spark.stop()
  }
}
