package io.semanticdf.mcp

import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.server.McpServer
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification
import io.modelcontextprotocol.server.McpSyncServer
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities
import io.modelcontextprotocol.spec.McpSchema.Tool
import io.semanticdf.mcp.handlers.{DescribeModel, Introspect, ListModels, OrderByParser, Query, QueryRequest}

import java.util.{List => JList}
import org.apache.spark.sql.SparkSession

/** Server construction — wires the stdio transport, the SDK's McpServer, and
  * the registered tools.
  *
  * Tools registered, in MCP-1 / MCP-1b / MCP-1c order:
  *   - `list_models`    (MCP-1a): bootstrap — reports loaded models
  *   - `describe_model` (MCP-1b): full schema + okf_markdown sidecar
  *   - `query`          (MCP-1c): runs the query, returns rows
  *   - `explain`        (MCP-1c): same shape, no execution
  *   - `introspect`     (MCP-1d): follows in a separate PR
  *
  * All tool handlers are thin adapters that forward to the corresponding
  * library method (`YamlLoader.loadDir`, `SemanticTable.query`,
  * `SemanticTable.explainSemantic`, `Introspector.fromFile`).
  *
  * Errors travel as JSON envelopes — every tool's output is wrapped in
  * [[Envelope]] (success) or [[ErrorEnvelope]] (failure). The SDK adapter
  * in each handler maps domain exceptions to the closed error-code list
  * in `docs/agents/mcp-contract.md` v2. */
object Server {

  def build(
      models: Models,
      okf: OkfCache,
      spark: SparkSession,
      mapper: McpJsonMapper,
  ): McpSyncServer = {
    val transport = new StdioServerTransportProvider(mapper)
    val queryHandler = new Query(spark)

    McpServer.sync(transport)
      .serverInfo("semanticdf-mcp", "0.1.8")
      .capabilities(
        ServerCapabilities.builder()
          .tools(true)
          .logging()
          .build(),
      )
      .tools(JList.of(
        ListModels.registerSpec(models, mapper),
        DescribeModel.registerSpec(models, okf, mapper),
        Query.registerQuerySpec(models, queryHandler, mapper),
        Query.registerExplainSpec(models, queryHandler, mapper),
        Introspect.registerSpec(models, new Introspect(spark), mapper),
      ))
      .build()
  }
}
