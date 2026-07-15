package io.semanticdf.mcp

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities
import io.semanticdf.mcp.handlers.{DescribeModel, ListModels}

import java.util.{List => JList}

/** Server construction — wires the stdio transport, the SDK's McpServer, and
  * the registered tools.
  *
  * Tools registered, in MCP-1 / MCP-1b order:
  *   - `list_models`    (MCP-1a): bootstrap — reports loaded models
  *   - `describe_model` (MCP-1b): full schema + okf_markdown sidecar
  *   - `query`, `explain`, `introspect` (MCP-1c / -1d): follow-on PRs
  *
  * The handler output is JSON-serialised into the MCP `TextContent` payload.
  * Per `mcp-contract.md` v2 §"Result envelope", the JSON is always wrapped in
  * the [[Envelope]] shape — agents parse one shape, not two. */
object Server {

  def build(models: Models, okf: OkfCache, mapper: McpJsonMapper): McpSyncServer = {
    val transport = new StdioServerTransportProvider(mapper)

    McpServer.sync(transport)
      .serverInfo("semanticdf-mcp", "0.1.0")
      .capabilities(
        ServerCapabilities.builder()
          .tools(true)            // tool list may grow in subsequent PRs
          .logging()
          .build(),
      )
      .tools(JList.of(
        ListModels.registerSpec(models, mapper),
        DescribeModel.registerSpec(models, okf, mapper),
      ))
      .build()
  }
}
