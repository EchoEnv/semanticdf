package io.semanticdf.mcp.handlers

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.spec.McpSchema.Tool
import io.semanticdf.MeasureKind
import io.semanticdf.mcp.{Envelope, ErrorEnvelope, Handlers, Models, OkfCache}

/** `describe_model` handler — full schema of one model. The agent gets
  * dimensions, measures, joins (with one-liner summaries), filters, version,
  * and (optionally) the OKF markdown sidecar.
  *
  * Per `mcp-contract.md` v2 §"Tool 2: describe_model":
  *
  *   Request:    `{"model": "flights", "include_okf": true}`  (include_okf defaults true)
  *   Response:   `{"model", "version", "description", "source_table",
  *                  "filters", "dimensions", "measures", "joins",
  *                  "okf_markdown"}` (with okf_markdown omitted when
  *                  include_okf is false)
  *
  * The handler is a thin adapter — every field comes from a corresponding
  * accessor on `SemanticTable` that PR #6 and PR #17 added. No data
  * parsing or schema introspection happens here. */
final class DescribeModel {

  import DescribeModel._

  /** Top-level response data shape, mirroring the contract. */
  final case class Data(
      model: String,
      version: Int,
      description: String,
      source_table: Option[String],
      status: String,
      filters: List[FilterEntry],
      dimensions: List[DimensionEntry],
      measures: List[MeasureEntry],
      joins: List[JoinEntry],
      okf_markdown: Option[String] = None,
  )

  final case class FilterEntry(
      name: String,
      description: String,
      expr: String,
      metadata: Map[String, String],
  )

  final case class DimensionEntry(
      name: String,
      // `expr` is the original expression string when known (YAML `expr:` value
      // or programmatic hint). Falls back to the lambda's `toString` for
      // backwards compatibility with consumers that build dimensions via bare
      // lambdas with no hint — the result is opaque `Lambda$...` addresses
      // in that case. Prefer carrying the source string for human-readable output.
      expr: String,
      description: String,
      metadata: Map[String, String],
      is_entity: Boolean,
      is_time_dimension: Boolean,
      smallest_time_grain: Option[String],
  )

  final case class MeasureEntry(
      name: String,
      kind: String,           // "base" | "calc"
      expr: String,
      description: String,
      metadata: Map[String, String],
  )

  final case class JoinEntry(
      name: String,
      cardinality: String,    // "one" | "many" | "cross"
      left: String,
      right: String,
      keys: List[String],
      summary: String,        // one-liner, see buildJoinSummary
      extra_dimensions: List[String],
      extra_measures: List[String],
  )

  /** Handle the request. Errors travel as [[ErrorEnvelope]] for transport;
    * the handler raises [[DescribeModel.ModelNotFoundError]] for the
    * `MODEL_NOT_FOUND` case (the SDK adapter maps it). The registry's own
    * [[io.semanticdf.mcp.ModelNotFound]] exception is caught here and
    * re-thrown as the more-specific handler error so the SDK adapter only
    * catches one type per handler. */
  def handle(registry: Models, okf: OkfCache, modelName: String, includeOkf: Boolean): Envelope[Data] = {
    val t = try {
      registry(modelName)
    } catch {
      case e: io.semanticdf.mcp.ModelNotFound =>
        throw DescribeModel.ModelNotFoundError(modelName, e.available)
    }

    val data = Data(
      model        = modelName,
      version      = t.version,
      description  = t.description.getOrElse(""),
      source_table = t.sourceTable,
      status       = t.status.asString,
      filters      = t.filters.toList.map { f => FilterEntry(f.name, f.description.getOrElse(""), f.expr, f.metadata) },
      dimensions   = t.dimensions.toList.sortBy(_._1).map { case (_, d) =>
        DimensionEntry(
          name = d.name,
          expr = d.exprString.getOrElse(d.expr.toString),
          description = d.description.getOrElse(""),
          metadata = d.metadata,
          is_entity = d.isEntity,
          is_time_dimension = d.isTimeDimension,
          smallest_time_grain = d.smallestTimeGrain,
        )
      },
      measures = t.measures.toList.sortBy(_._1).map { case (name, m) =>
        MeasureEntry(
          name = name,
          kind = describeKind(t.measureKind(name)),
          expr = m.exprString.getOrElse(m.expr.toString),
          description = m.description.getOrElse(""),
          metadata = m.metadata,
        )
      },
      joins = t.joins.toList.map { j =>
        val (leftLabel, rightLabel) = (j.leftName.getOrElse("left"), j.rightName.getOrElse("right"))
        JoinEntry(
          name = j.cardinality + "_" + leftLabel + "_" + rightLabel,
          cardinality = j.cardinality,
          left = leftLabel,
          right = rightLabel,
          keys = j.keys.toList,
          summary = buildJoinSummary(j.cardinality, leftLabel, rightLabel, j.keys.toList),
          extra_dimensions = j.extraDimensions.toList,
          extra_measures = j.extraMeasures.toList,
        )
      },
      okf_markdown = if (includeOkf) okf(modelName) else None,
    )
    Envelope.ok(data, warnings = Handlers.lifecycleWarnings(modelName, t.status))
  }

  /** `MeasureKind.Base` → `"base"`, `MeasureKind.Calc` → `"calc"`.
    * Strings (not sealed-trait names) so the JSON shape is wire-stable. */
  private def describeKind(k: MeasureKind): String = k match {
    case MeasureKind.Base => "base"
    case MeasureKind.Calc => "calc"
    case other => other.toString  // future-proof: log unexpected kinds
  }
}

/** Companion: top-level utilities that don't depend on a `DescribeModel`
  * instance. Lifted here so the tests (in the same sub-package) can call
  * `buildJoinSummary` directly via the companion. */
object DescribeModel {

  /** Build the join one-liner per `mcp-contract.md` v2 §"Join one-liner rule":
    *
    *   single key:   `flights.carrier → carriers.carrier (one)`
    *   composite:    `orders.(order_id, line_id) → line_items.(order_id, line_id) (many)`
    *   cross:        `a. → b. (cross)`
    *
    * Keys absent (cross join) renders as empty `.` after each side. */
  def buildJoinSummary(
      cardinality: String,
      leftLabel: String,
      rightLabel: String,
      keys: List[String],
  ): String = {
    val leftKey  = renderKey(leftLabel, keys)
    val rightKey = renderKey(rightLabel, keys)
    s"$leftKey → $rightKey ($cardinality)"
  }

  private def renderKey(label: String, keys: List[String]): String = keys match {
    case Nil                   => s"$label."
    case head :: Nil           => s"$label.$head"
    case _ :: _ :: _           => s"$label.(${keys.mkString(", ")})"
    case _                    => s"$label."  // catch-all safety net (unreachable for inputs with ≥1 key)
  }

  /** Thrown by the handler when the model name is not in the registry.
    * The MCP adapter catches this by type and serialises it as a
    * `MODEL_NOT_FOUND` error envelope (per the closed list in
    * `mcp-contract.md` v2 §"Error codes"). */
  final case class ModelNotFoundError(name: String, available: String)
      extends RuntimeException(s"describe_model: no model named '$name'. Available: $available")

  /** Register the `describe_model` tool in the MCP server. The handler reads
    * `arguments` as a `Map[String, Object]` (per the SDK 0.18.x BiFunction
    * signature); we pull `model` and the optional `include_okf` flag.
    *
    * Reading-time validation: the SDK enforces `modelNameSchema` (model is a
    * required string). The `include_okf` flag is optional and defaults to
    * true when absent — see `handler.handle`. */
  def registerSpec(
      registry: Models,
      okf: OkfCache,
      mapper: McpJsonMapper,
  ): SyncToolSpecification = {
    val handler = new DescribeModel()

    val tool = new Tool.Builder()
      .name("describe_model")
      .description(
        "Full schema of one model: dimensions, measures (with kind: base/calc), " +
        "joins (with one-liner summaries), filters, version, source_table, " +
        "and (optionally) an OKF sidecar markdown."
      )
      .inputSchema(Handlers.modelNameSchema)
      .build()

    new SyncToolSpecification(
      tool,
      (_exchange: io.modelcontextprotocol.server.McpSyncServerExchange, arguments: java.util.Map[String, Object]) => {
        val modelName = arguments.get("model") match {
          case s: String => s
          case null      => throw new IllegalArgumentException("describe_model: 'model' argument is required")
          case other     => throw new IllegalArgumentException(s"describe_model: 'model' must be a string, got ${other.getClass.getSimpleName}")
        }
        val includeOkf = arguments.get("include_okf") match {
          case null   => true                              // default: include the okf_markdown
          case b: java.lang.Boolean => b.booleanValue
          case other => throw new IllegalArgumentException(s"describe_model: 'include_okf' must be a boolean, got ${other.getClass.getSimpleName}")
        }
        // Inner try-catch: convert ModelNotFound (and any handler errors)
        // to the MCP error envelope shape before serialising.
        try {
          Handlers.textResult(handler.handle(registry, okf, modelName, includeOkf), mapper)
        } catch {
          case e: ModelNotFoundError =>
            Handlers.textResult(
              ErrorEnvelope.of(
                code = "MODEL_NOT_FOUND",
                message = e.getMessage,
                hint = io.semanticdf.closestMatch(modelName, registry.all.map(_._1)).map(c => s"Did you mean '$c'?"),
              ),
              mapper,
            )
        }
      },
    )
  }
}
