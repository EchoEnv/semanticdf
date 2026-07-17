package io.semanticdf.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.modelcontextprotocol.json.McpJsonMapper
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper

/** JSON support helpers shared across the MCP server module.
  *
  * The single entry point is [[scalaMapper]] — a Jackson `ObjectMapper`
  * with the Scala module registered, wrapped in an [[McpJsonMapper]].
  *
  * == Why this exists ==
  *
  * The MCP SDK's [[io.modelcontextprotocol.json.McpJsonDefaults.getMapper]]
  * returns a *plain* Jackson mapper (no Scala module). This means our
  * generic case classes (`Envelope[T]`, `ErrorEnvelope`, `Meta`, etc.)
  * serialize as `{}` because Jackson can't introspect the type
  * parameters of a generic case class without the Scala module's help.
  *
  * The REST and MCP layers both use this mapper so the JSON wire format
  * is consistent across transports.
  *
  * == Why this works (Jackson version conflict resolution) ==
  *
  * Spark 3.5.x bundles `jackson-module-scala 2.15.2`, which strictly
  * requires `jackson-databind 2.15.0–2.16.0` (it does a `setupModule`
  * version-range check and throws otherwise). The MCP SDK's
  * `mcp-json-jackson2` declares `jackson-databind 2.20.1`. We pin
  * `jackson-databind` to 2.15.2 in the MCP module's pom, which lets the
  * Scala module load cleanly and gives us a single, consistent Jackson
  * version across the whole server. */
private[mcp] object JsonSupport {

  /** Build a Jackson mapper with the Scala module registered, wrapped as
    * an [[McpJsonMapper]]. Used as the default mapper for the REST layer
    * and as the response serializer for the MCP layer.
    */
  def scalaMapper(): McpJsonMapper = {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    new JacksonMcpJsonMapper(om)
  }
}