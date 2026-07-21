package io.semanticdf.mcp

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.spec.McpSchema.CallToolResult
import io.modelcontextprotocol.spec.McpSchema.JsonSchema
import io.modelcontextprotocol.spec.McpSchema.TextContent
import io.semanticdf.{ModelStatus, SemanticTable}
import java.util.{List => JList}

/** Result envelope — every tool's `data` payload travels inside this.
  *
  * Mirrors the `mcp-contract.md` v2 §"Result envelope". The `status` field
  * is `"ok"` for successful tool calls and `"error"` for failures (errors
  * also travel inside the same `Envelope` shape — see [[ErrorEnvelope]]).
  *
  * All JSON serialisation goes through the MCP SDK's McpJsonMapper, which
  * is Jackson under the hood. Case-class field names map 1:1 to JSON
  * property names, so `{"status": "ok", "data": ...}` is produced verbatim. */
final case class Envelope[T](
    status: String,           // "ok" | "error"
    data: T,                  // tool-specific payload (only on success)
    warnings: List[String] = Nil,
    meta: Meta = Meta(),
) {
  def map[U](f: T => U): Envelope[U] = copy(data = f(data))
}

object Envelope {
  def ok[T](data: T, warnings: List[String] = Nil, meta: Meta = Meta()): Envelope[T] =
    Envelope(status = "ok", data = data, warnings = warnings, meta = meta)
}

/** Error envelope — `data` is absent, `error` carries the typed failure. */
final case class ErrorEnvelope(
    status: String,           // always "error"
    error: ErrorDetail,
)

object ErrorEnvelope {
  def of(code: String, message: String, hint: Option[String] = None): ErrorEnvelope =
    ErrorEnvelope(status = "error", error = ErrorDetail(code, message, hint))
}

/** One typed failure. `code` is drawn from the closed list in mcp-contract.md. */
final case class ErrorDetail(
    code: String,             // e.g. "MODEL_NOT_FOUND"
    message: String,          // human-readable
    hint: Option[String] = None,
    details: Map[String, String] = Map.empty,
)

/** Per-request metadata. `elapsed_ms` is filled by the handler; the other
  * fields are tool-specific (rows in / rows out / bytes scanned / model
  * name). Empty default keeps the envelope small for the common case. */
final case class Meta(
    elapsed_ms: Long = 0L,
    rows_in_result: Option[Int] = None,
    rows_scanned: Option[Int] = None,
    truncated: Boolean = false,
    model: Option[String] = None,
)

/** Internal helpers shared by all handler implementations. */
object Handlers {

  /** Wrap any value into a `CallToolResult` whose single `TextContent` is
    * the JSON-serialised form. Used by every tool handler to deliver its
    * [[Envelope]] to the MCP transport. */
  def textResult[T](value: T, mapper: McpJsonMapper): CallToolResult = {
    val json = mapper.writeValueAsString(value)
    CallToolResult.builder()
      .content(JList.of(new TextContent(json)))
      .build()
  }

  /** JSON Schema for "this tool takes one required string argument named `model`".
    * The MCP SDK validates the agent's `arguments` map against this schema
    * (default-true per the SDK's per-tool input validation).
    */
  def modelNameSchema: JsonSchema = new JsonSchema(
    "object",
    java.util.Map.of(
      "model", java.util.Map.of(
        "type", "string",
        "description", "Model name to describe (must match a model registered via YamlLoader.loadDir)."
      ),
    ),
    JList.of("model"),
    java.lang.Boolean.FALSE,
    java.util.Map.of(),
    java.util.Map.of(),
  )

  /** JSON Schema for "this tool takes no arguments".
    * `{"type": "object", "properties": {}, "required": [], "additionalProperties": false}` —
    * the canonical shape per JSON Schema 2020-12. */
  def emptySchema: JsonSchema = new JsonSchema(
    "object",
    java.util.Map.of(),
    JList.of(),
    java.lang.Boolean.FALSE,
    java.util.Map.of(),
    java.util.Map.of(),
  )

  /** Build a lifecycle warning list for a single model.
    *
    * Takes the registry key (canonical name the agent called the model
    * with) — not `t.name`, because `t.name` may be empty for anonymous
    * models or differ from the registry key.
    *
    * Returns `Nil` for `Published` (the default; no signal). Returns one
    * display-text string per non-Published status. Wire-stable lowercase
    * strings intended for LLM consumption; see recipe §4 for the format
    * contract.
    *
    * Used by Query.handle, Query.explain, ListModels.handle (per-model),
    * DescribeModel.handle. All four call sites share this single helper.
    */
  def lifecycleWarnings(modelName: String, status: ModelStatus): List[String] = status match {
    case ModelStatus.Deprecated =>
      List(s"model '$modelName' is deprecated")
    case ModelStatus.Draft =>
      List(s"model '$modelName' is in draft; shape may change")
    case ModelStatus.Published =>
      Nil
  }
}
