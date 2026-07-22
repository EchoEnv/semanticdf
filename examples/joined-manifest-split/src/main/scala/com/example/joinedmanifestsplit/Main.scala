package com.example.joinedmanifestsplit

import io.semanticdf._
import io.semanticdf.SemanticManifest
import io.semanticdf.SemanticManifest.Identity

import org.apache.spark.sql.SparkSession

/** Worked example: emit per-side single-table manifests from a joined
  * YAML model.
  *
  * ==Why this example exists==
  *
  * In v0.1.11 the `SemanticManifest.toJson` writer deliberately rejects
  * joined-rooted models (anti-scope, recipe §10). The
  * `joined-models-manifest` recipe is BLOCKed on `SemanticJoinOp` not
  * carrying enough side metadata. Until that lands, operators who want
  * a portable record of a joined model emit one single-table manifest
  * per side and hand-compose the joined envelope themselves.
  *
  * ==What this demo does==
  *
  *   1. Load a directory of YAMLs that contains a joined model
  *      (`orders` joins `customers`).
  *   2. Confirm `orders` is `SemanticJoinOp`-rooted (the writer would
  *      throw on it directly).
  *   3. Emit per-side single-table manifests using
  *      `SemanticManifest.sideIdentity(parent, "left", "<name>")`.
  *   4. Hand-compose the joined envelope (the wire shape the BLOCKed
  *      `joined-models-manifest` recipe will eventually produce
  *      natively).
  *
  * Run:
  * {{{
  *   cd examples/joined-manifest-split
  *   mvn -o package
  *   mvn -o exec:java -Dexec.mainClass=com.example.joinedmanifestsplit.Main
  * }}}
  */
object Main {

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("joined-manifest-split")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("[demo] === joined-manifest-split demo ===")

    // ------------------------------------------------------------------
    // Step 1: register the source CSVs as views the YAML references.
    // ------------------------------------------------------------------
    val dataDir = "data"
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/customers.csv").createOrReplaceTempView("customers_csv")
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"$dataDir/orders.csv").createOrReplaceTempView("orders_csv")
    println("[demo] registered temp views: customers_csv, orders_csv")

    // ------------------------------------------------------------------
    // Step 2: load YAMLs. `orders` becomes joined-rooted in memory.
    // ------------------------------------------------------------------
    val all = YamlLoader.loadDir("models", spark)
    println(s"[demo] loaded ${all.size} model(s): ${all.keys.mkString(", ")}")
    val orders = all("orders")
    assert(orders.isJoined,
      "this demo expects the orders YAML to join customers; runtime confirms")

    // ------------------------------------------------------------------
    // Step 3: confirm the writer rejects the joined root (anti-scope).
    // Use `isJoined` to branch cleanly — no try/catch in user code.
    // ------------------------------------------------------------------
    val parentIdentity = Identity(
      id              = "io.example.joinedmanifestsplit.orders",
      manifestVersion = SemanticManifest.InitialManifestVersion,
      namespace       = "demo",
      metadata        = Map("owner" -> "data-platform", "demo" -> "joined-manifest-split"),
    )
    if (orders.isJoined) {
      println("[demo] orders.isJoined == true; the writer (toJson) would throw" +
        " (anti-scope §10). Branching on isJoined avoids the throw.")
    } else {
      // In a real project you'd just call SemanticManifest.toJson(orders, parentIdentity)
      // without any guard, but in this demo the model is joined so we exercise the
      // per-side workflow.
    }

    // ------------------------------------------------------------------
    // Step 4: emit per-side manifests.
    // ------------------------------------------------------------------
    // Re-load each YAML independently so we capture each side's pre-join
    // (single-table) state. The customers.yml is single-table. The
    // orders.yml is joined-rooted, so for a real project you'd ship a
    // separate `orders-source.yml` (no joins) backing the right side.
    // In this demo we emit a hand-rolled stub for the orders side that
    // uses the same `id`/`namespace`/`metadata` derivation as a real
    // single-table manifest.
    val customersTbl = YamlLoader.load("models/customers.yml", spark)("customers")

    val customersId = SemanticManifest.sideIdentity(parentIdentity, "left",  "customers")
    val ordersId    = SemanticManifest.sideIdentity(parentIdentity, "right", "orders")

    val customersJson = SemanticManifest.toJson(customersTbl, customersId, prettyPrint = true)

    // The orders side stub: in a real project, this comes from
    // `SemanticManifest.toJson(ordersSource, ordersId)` against a
    // non-joined orders-source YAML. Here we build it from scratch.
    val ordersStub = orderSideStub(ordersId)
    println(s"[demo] per-side customers manifest: ${customersJson.length} bytes, id=${customersId.id}")
    println(s"[demo] per-side orders manifest:    ${ordersStub.length} bytes, id=${ordersId.id}")

    // ------------------------------------------------------------------
    // Step 5: verify each side parses cleanly via parseMeta.
    // ------------------------------------------------------------------
    for ((label, json) <- Seq("customers" -> customersJson, "orders" -> ordersStub)) {
      val meta = SemanticManifest.parseMeta(json)
      println(s"[demo] $label manifest header: schemaVersion=${meta.schemaVersion}" +
        s" kind=${meta.kind} id=${meta.id.getOrElse("<none>")}" +
        s" namespace=${meta.namespace.getOrElse("<none>")}" +
        s" dims=${meta.dimensions} measures=${meta.measures}")
    }

    // ------------------------------------------------------------------
    // Step 6: hand-compose the joined envelope (BLOCKed recipe shape).
    // ------------------------------------------------------------------
    val customersBody = trimBraces(customersJson)
    val ordersBody    = trimBraces(ordersStub)
    val joinedEnvelope =
      s"""{
         |  "schemaVersion":  "${SemanticManifest.CurrentSchemaVersion}",
         |  "kind":            "semanticdf-joined-manifest",
         |  "compiledAt":      "${java.time.Instant.now}",
         |  "model": {
         |    "name":        "orders",
         |    "version":     0,
         |    "status":      "${orders.status.asString}",
         |    "description": "Orders enriched with customer details",
         |    "left":        { $customersBody },
         |    "right":       { $ordersBody },
         |    "join": {
         |      "cardinality": "one",
         |      "leftKeys":    ["customer_id"],
         |      "rightKeys":   ["customer_id"]
         |    }
         |  },
         |  "warnings": []
         |}
         |""".stripMargin
    println(s"[demo] hand-rolled joined envelope: ${joinedEnvelope.length} bytes")

    // ------------------------------------------------------------------
    // Step 7: write all three artifacts to disk under target/manifests/.
    // ------------------------------------------------------------------
    val outDir = new java.io.File("target/manifests")
    outDir.mkdirs()
    writeUtf8(new java.io.File(outDir, "customers.json"),                customersJson)
    writeUtf8(new java.io.File(outDir, "orders.json"),                   ordersStub)
    writeUtf8(new java.io.File(outDir, "orders.joined-envelope.json"),   joinedEnvelope)
    println(s"[demo] artifacts written to ${outDir.getAbsolutePath}")
    println("[demo] === joined-manifest-split demo complete ===")
    spark.stop()
  }

  /** Trim the leading/trailing `{` / `}` so we can inline the per-side
    * manifest as the `left`/`right` values of the envelope. */
  private def trimBraces(s: String): String =
    s.trim match {
      case body if body.startsWith("{") && body.endsWith("}") => body.substring(1, body.length - 1)
      case other                                              => other
    }

  /** Build a hand-rolled single-table manifest stub for the orders side.
    * In a real project this is `SemanticManifest.toJson(ordersSource, id)`
    * against a non-joined orders-source YAML. The stub uses the same
    * `id`/`namespace`/`metadata` derivation so the joined envelope is
    * shaped correctly. */
  private def orderSideStub(id: Identity): String = {
    val metaJson = id.metadata.toList.map { case (k, v) =>
      s""""$k":${jsonString(v)}"""
    }.mkString("{", ",", "}")
    s"""{
       |  "schemaVersion":   "${SemanticManifest.CurrentSchemaVersion}",
       |  "kind":            "semanticdf-model-manifest",
       |  "compiledAt":      "${java.time.Instant.now}",
       |  "manifestVersion": "${SemanticManifest.InitialManifestVersion}",
       |  "id":              "${id.id}",
       |  "namespace":       "${id.namespace}",
       |  "metadata":        $metaJson,
       |  "model": {
       |    "name":        "orders-source",
       |    "version":     0,
       |    "status":      "draft",
       |    "description": "Stub for the orders side of the join (real project: orders-source.yml)",
       |    "sourceTable": "orders_csv"
       |  },
       |  "digest": {
       |    "dimensions": 4, "timeDimensions": 0, "derivedTimeDimensions": 0,
       |    "measures":   2, "calcMeasures":    0,
       |    "joins": 0, "filters": 0, "isStreaming": false, "usesTAll": false
       |  },
       |  "dimensions": [],
       |  "measures":   [],
       |  "joins":      [],
       |  "filters":    []
       |}
       |""".stripMargin
  }

  private def jsonString(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def writeUtf8(f: java.io.File, body: String): Unit = {
    val pw = new java.io.PrintWriter(f, "UTF-8")
    try pw.write(body) finally pw.close()
  }
}
