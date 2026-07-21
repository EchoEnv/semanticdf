package io.semanticdf.mcp.handlers

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.spec.McpSchema.{CallToolResult, Tool}
import io.modelcontextprotocol.spec.McpSchema.{JsonSchema => McpJsonSchema}
import io.semanticdf.mcp.{Envelope, Handlers, Models, OkfCache}
import io.semanticdf.tools.Introspector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.util.{List => JList}
import scala.jdk.CollectionConverters._

/** `introspect` tool — auto-generate a starter YAML model from a DataFrame the
  * agent hasn't modeled yet.
  *
  * Per `mcp-contract.md` v2 §"Tool 5: introspect":
  *
  *   Request:    `{table, format, path, model_name, read_options}`
  *   Response:   `{yaml, field_inventory, warnings}`
  *
  * Library call (per the contract):
  *
  *   Introspector(spark = spark).fromFile(
  *     spark  = spark,
  *     path   = req.path,
  *     format = req.format,
  *     modelName = req.model_name,
  *     readOptions = req.read_options)
  *
  * Today the library returns the YAML string only — `field_inventory` and
  * `warnings` are derived in the handler from a small YAML-pattern scan.
  * Both are append-only and reviewable in a follow-up if the contract
  * demands richer metadata. */
final class Introspect(spark: SparkSession) {

  import Introspect._

  private val log = Introspect.log

  /** Result data shape. Mirrors the contract. */
  final case class Data(
      yaml: String,
      field_inventory: Introspect.FieldInventory,
      warnings: Seq[String],
  )

  /** Handle `introspect`: read the file via Spark, generate starter YAML,
    * wrap the result. */
  def handle(
      registry: Models,
      table: Option[String],
      format: Option[String],
      path: Option[String],
      modelName: Option[String],
      readOptions: Map[String, String] = Map.empty,
  ): Envelope[Data] = {
    // Defaults: per the contract, only `path` and `model_name` are required.
    // `table` and `format` are optional. We use:
    //   - format:    default "parquet"  (library's default)
    //   - table:     ignored here; the result is a starter YAML that the agent
    //                reviews. (It IS surfaced in the response as a stub for
    //                the `table:` field of the generated YAML.)
    //   - path:      required — we use it as the file location for fromFile
    val resolvedPath     = path.getOrElse(sys.error("introspect: 'path' is required"))
    val resolvedModelName = modelName.getOrElse(sys.error("introspect: 'model_name' is required"))
    val resolvedFormat    = format.getOrElse("parquet")
    val resolvedTable     = table.getOrElse(resolvedModelName)   // table → model key, fallback to model name

    log.info(s"introspect: format=$resolvedFormat path=$resolvedPath model=$resolvedModelName")

    val yaml = new Introspector()
      .fromFile(spark, resolvedPath, resolvedFormat, resolvedModelName, readOptions)

    val inventory = parseInventory(yaml)
    val warnings  = parseWarnings(yaml)   // extracts `# WARN:` lines Introspector emits for skipped fields

    val data = Data(
      yaml           = stubTableField(yaml, resolvedTable),
      field_inventory = inventory,
      warnings       = warnings,
    )

    Envelope.ok(
      data,
      meta = io.semanticdf.mcp.Meta(model = Some(resolvedModelName)),
    )
  }

  // ---------------------------------------------------------------------------
  // SDK adapter
  // ---------------------------------------------------------------------------

  // (The `introspectSchema` definition lives in the companion `Introspect`
  // object — see below. It's referenced from `registerSpec`.)

  // ---------------------------------------------------------------------------
  // Helpers — derive inventory + warnings from the YAML
  // ---------------------------------------------------------------------------

  /** Replace the placeholder `table: <source name>` line with the
    * resolved `table` value, if the agent passed one. The library
    * intentionally leaves it as a hint for the user to fill in. */
  private def stubTableField(yaml: String, table: String): String =
    yaml.replaceFirst("""(?m)^  table: <source name>.*$""", s"  table: $table  # ← from agent's request")
}

object Introspect {
  private[handlers] val log = org.slf4j.LoggerFactory.getLogger(classOf[Introspect])

  /** Top-level request DTO. */
  final case class Request(
      table: Option[String] = None,
      format: Option[String] = None,
      path: Option[String] = None,
      model_name: Option[String] = None,
      read_options: Map[String, String] = Map.empty,
  )

  /** Field inventory counts — derived from the generated YAML. Mirrors
    * the `field_inventory` field of the contract's response. */
  final case class FieldInventory(
      dimensions: Int,
      measures: Int,
      skipped: Int,
  )

  /** Parse the YAML to count dimensions, measures, and the remaining
    * (skipped) fields. Recognises the YAML shape that `Introspector`
    * emits — see `src/main/scala/io/semanticdf/tools/Introspector.scala`.
    *
    * Single source of truth: `skipped` is derived from the same
    * `# WARN:` lines that [[parseWarnings]] extracts — not from
    * `total - declared`. The old formula could under-count because a
    * numeric entity column can appear in BOTH the `measures:` section
    * (numeric measure) AND the `joins:` section (entity placeholder),
    * inflating `declared` past the real column count and clamping
    * `skipped` to a smaller value than the truth. */
  def parseInventory(yaml: String): Introspect.FieldInventory = {
    val dimRe   = """(?m)^  dimensions:""".r
    val meaRe   = """(?m)^  measures:""".r
    val nRowsRe = """(?m)^# (\d+) rows, (\d+) columns\.""".r

    // Total columns: from the `# N rows, M columns.` header line.
    // (Currently unused for the skipped count — the header line can
    // be absent (malformed YAML) and we don't want skipped to clamp
    // to 0 in that case when warnings clearly exist. Kept for
    // potential future use / observability.)

    // Count top-level entries under each section. We look for lines that
    // begin with `    <name>:` (4-space indent, a YAML mapping key).
    //
    // The end-boundary check uses `\n  [a-zA-Z]` (newline + 2 spaces + a
    // letter) so we don't accidentally match the section content's own
    // leading newline-and-spaces (`\n    carrier:` starts with `  ` too).
    def countSection(begin: scala.Option[scala.util.matching.Regex.Match]): Int = begin match {
      case None => 0
      case Some(m) =>
        val startIdx = m.end
        val rest = yaml.substring(startIdx)
        // Look for the next *top-level* section header. `[a-zA-Z]` rules out
        // the section content (which starts with 4-space-indent entries).
        val endRe = """\n  [a-zA-Z]""".r
        val endIdx = endRe.findFirstMatchIn(rest).map(_.start).getOrElse(rest.length)
        val section = rest.substring(0, endIdx)
        val entryRe = """(?m)^    [a-zA-Z_][a-zA-Z_0-9]*:""".r
        entryRe.findAllMatchIn(section).length
    }

    val dims   = countSection(dimRe.findFirstMatchIn(yaml))
    val meas   = countSection(meaRe.findFirstMatchIn(yaml))
    // Joins section isn't counted separately as a 4th inventory bucket
    // (the MCP contract has only dimensions/measures/skipped). It's
    // folded in implicitly: a column that ends up only in `joins:` is
    // not classified as a dim nor a measure, but `parseWarnings` would
    // still surface it if it were skipped. In practice entity columns
    // get SOMETHING (dim or measure) in the same model, so the joins
    // section is decoration that doesn't change the inventory totals.
    //
    // Skipped: count the `# WARN:` lines. This agrees with `parseWarnings`
    // by construction — single source of truth — and avoids the
    // over-count of `total - declared`.
    val skipped = parseWarnings(yaml).length

    FieldInventory(
      dimensions = dims,
      measures   = meas,
      skipped    = skipped,
    )
  }

  /** Parse warnings out of the YAML header. The Introspector emits
    * `# WARN: field 'X' (type) was skipped — reason` lines for any field
    * that didn't get classified as a dimension, measure, or join. Each
    * WARN line becomes one entry in the returned seq (without the `# WARN:`
    * prefix, per the contract response shape).
    *
    * The regex is anchored to the start of a line so the same string
    * inside a quoted description doesn't accidentally match. */
  def parseWarnings(yaml: String): Seq[String] = {
    val warnRe = """(?m)^# WARN: (.+)$""".r
    warnRe.findAllMatchIn(yaml).map(_.group(1)).toSeq
  }

  /** JSON Schema for the `introspect` tool input. `path` and `model_name` are
    * the only required fields. */
  val introspectSchema: McpJsonSchema = new McpJsonSchema(
    "object",
    java.util.Map.of(
      "path"        , java.util.Map.of("type", "string"),
      "model_name"  , java.util.Map.of("type", "string"),
      "table"       , java.util.Map.of("type", "string"),
      "format"      , java.util.Map.of("type", "string"),
      "read_options", java.util.Map.of("type", "object"),
    ),
    JList.of("path", "model_name"),
    java.lang.Boolean.TRUE,    // extra props allowed (forward-compat)
    java.util.Map.of(),
    java.util.Map.of(),
  )

  /** Register the `introspect` tool with the MCP server. */
  def registerSpec(
      registry: Models,
      handler: Introspect,
      mapper: McpJsonMapper,
  ): SyncToolSpecification = {
    val tool = new Tool.Builder()
      .name("introspect")
      .description("Auto-generate a starter YAML model for a DataFrame the agent hasn't modeled yet. " +
                   "Reads the file via Spark, runs the library's heuristic inspector, returns the YAML, " +
                   "plus a field inventory (dimensions / measures / skipped) and warnings.")
      .inputSchema(introspectSchema)
      .build()

    new SyncToolSpecification(
      tool,
      (_exchange: io.modelcontextprotocol.server.McpSyncServerExchange, args: java.util.Map[String, Object]) => {
        try {
          val req = parseRequest(args)
          val env = handler.handle(registry, req.table, req.format, req.path, req.model_name, req.read_options)
          Handlers.textResult(env, mapper)
        } catch {
          case e: IllegalArgumentException =>
            Handlers.textResult(
              io.semanticdf.mcp.ErrorEnvelope.of("EXECUTION_ERROR", e.getMessage),
              mapper,
            )
        }
      },
    )
  }

  /** Parse the SDK's `Map[String, Object]` into a strongly-typed request.
    *
    * `private[mcp]` so [[io.semanticdf.mcp.RestServer]] (which lives in
    * the parent package) can call this — the REST transport needs the
    * same DTO mapping the MCP SDK adapter uses, without re-parsing. */
  private[mcp] def parseRequest(args: java.util.Map[String, Object]): Request = {
    import scala.jdk.CollectionConverters._
    val map = args.asScala.toMap.asInstanceOf[Map[String, Any]]
    def asString(name: String): Option[String] = map.get(name).collect { case s: String => s }
    def asMap(name: String): Map[String, String] = map.get(name) match {
      case Some(jm: java.util.Map[_, _]) =>
        jm.asScala.toMap.iterator.map { case (k, v) =>
          k.toString -> v.toString
        }.toMap
      case Some(sm: scala.collection.Map[_, _]) =>
        sm.iterator.map { case (k, v) => k.toString -> v.toString }.toMap
      case _ => Map.empty
    }
    Request(
      table        = asString("table"),
      format       = asString("format"),
      path         = asString("path"),
      model_name   = asString("model_name"),
      read_options = asMap("read_options"),
    )
  }
}
