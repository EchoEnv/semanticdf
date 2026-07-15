package io.semanticdf.mcp

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
