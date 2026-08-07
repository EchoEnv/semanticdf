package io.semanticdf.mcp

// (McpJsonDefaults no longer used; we use JsonSupport.scalaMapper which
//  registers the Jackson Scala module so generic case classes
//  like Envelope[T] serialize correctly.)
import org.apache.spark.sql.SparkSession
import io.semanticdf.tools.SdfSession
import io.semanticdf.spark.SparkEngineProvider
import org.slf4j.LoggerFactory

/** CLI entry point — `mvn scala:run -DmainClass=io.semanticdf.mcp.Main` or
  * `mvn exec:java -Dexec.mainClass=io.semanticdf.mcp.Main` (preferred —
  * `scala:run` leaks compiler args; see `docs/runtime-quickstart.md` Trap #2).
  *
  * Arguments (per `mcp-contract.md` v2 §"Server lifecycle"):
  *
  *   --models <dir>        directory of `*.yml` model files
  *   --data <file>         data-config YAML (see `DataConfig.fromFile`)
  *   --okf-bundle <dir>    where OkfGen writes the OKF markdowns (server
  *                         caches them in memory at startup)
  *
  * Stdout is reserved for JSON-RPC (MCP hard requirement). All logs go to
  * stderr. Spark's own logging is configured to be silent on stdout for the
  * same reason — the agent sees nothing but JSON-RPC frames.
  *
  * Exit codes:
  *   0  clean shutdown (SIGINT / SIGTERM)
  *   1  invalid arguments
  *   2  data-config parse error or model-load error
  *   3  server runtime exception
  */
object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parsed = parseArgs(args) match {
      case Right(c) => c
      case Left(err) =>
        System.err.println(s"semanticdf-mcp: $err")
        System.err.println(usage)
        sys.exit(1)
    }

    // Mute Spark on stdout — its launcher banner + executor logs would
    // corrupt the JSON-RPC stream. Stderr-only.
    System.setProperty("spark.driver.log.level", "WARN")
    System.setProperty("spark.executor.log.level", "WARN")
    System.setProperty("log4j2.rootLogger.level", "WARN")

    // --remote sc://host:port → Spark Connect (server-side JVM).
    // default → local in-process session (matches pre-Connect behavior).
    val spark = SdfSession.createFromEnv(
      appName       = s"semanticdf-${parsed.transport}",
      flagOverride  = parsed.remote,
    )

    // Register the shutdown hook IMMEDIATELY after spark is created so that
    // any throw between here and the try block still triggers spark.stop().
    // Without this, an exception during `DataConfig.fromFile` or
    // `Models.load` (e.g. malformed YAML) would leak the Spark session —
    // Spark's default cleanup only runs on JVM exit, not on early throws.
    @volatile var mcpServer: io.modelcontextprotocol.server.McpSyncServer = null
    @volatile var restServer: com.sun.net.httpserver.HttpServer = null
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      try {
        if (mcpServer != null) mcpServer.close()
        if (restServer != null) restServer.stop(0)
      } finally {
        try { spark.stop() } catch { case _: Throwable => /* best-effort */ }
      }
    }))

    try {
      val dataConfig = DataConfig.fromFile(parsed.dataConfig)
      val models     = Models.load(parsed.modelsDir, dataConfig, spark)
      val okf        = OkfCache.build(parsed.modelsDir, parsed.okfBundleDir)
      val mapper     = JsonSupport.scalaMapper()

      // PR 5c: construct the engine registry (per design §6.4 +
      // PR #402). For v1, we register only the Spark engine provider
      // (Trino and other providers are future work). The default is
      // "spark" so existing clients get the legacy-equivalent path
      // (which the spark engine provider implements).
      val engineRegistry: io.semanticdf.core.engine.MCPEngineRegistry = {
        val sparkProvider = new SparkEngineProvider(
          spark, models.registry,
        )
        io.semanticdf.core.engine.MCPEngineRegistry(
          engines = Map("spark" -> sparkProvider),
          default = "spark",
        )
      }

      parsed.transport match {
        case "stdio" =>
          mcpServer = Server.build(models, okf, spark, mapper, engineRegistry = Some(engineRegistry))
          log.info("semanticdf-mcp listening on stdio. Press Ctrl-D to stop.")
          Thread.currentThread().join()  // park until SIGINT / SIGTERM

        case "rest" =>
          val rest = new RestServer(spark, models, okf, mapper, port = parsed.restPort)
          restServer = rest.start()
          log.info(s"semanticdf-rest listening on http://localhost:${parsed.restPort}")
          Thread.currentThread().join()  // park until SIGINT / SIGTERM

        case other =>
          System.err.println(s"semanticdf-mcp: unknown transport: $other")
          sys.exit(1)
      }
    } catch {
      case e: IllegalArgumentException =>
        System.err.println(s"semanticdf-mcp: configuration error: ${e.getMessage}")
        sys.exit(2)
      case e: Throwable =>
        System.err.println(s"semanticdf-mcp: server error: ${e.getClass.getSimpleName}: ${e.getMessage}")
        e.printStackTrace(System.err)
        sys.exit(3)
    }
  }

  // ---------------------------------------------------------------------------
  // CLI parsing — minimal hand-rolled, no library. We need three flags;
  // everything else is a usage error. Adding a flag parser would grow the
  // dependency surface for three flags.
  // ---------------------------------------------------------------------------

  private case class Config(
      modelsDir: String,
      dataConfig: String,
      okfBundleDir: String,
      transport: String,
      restPort: Int,
      /** Spark Connect URL (None → local). The MCP becomes a thin
        * gRPC client to an external Spark Connect server when set. */
      remote: Option[String] = None,
  )

  private def parseArgs(args: Seq[String]): Either[String, Config] = {
    @scala.annotation.tailrec
    def loop(it: List[String], acc: Config): Either[String, Config] = it match {
      case Nil => Right(acc)
      case "--models"     :: v :: rest if v.nonEmpty => loop(rest, acc.copy(modelsDir = v))
      case "--data"       :: v :: rest if v.nonEmpty => loop(rest, acc.copy(dataConfig = v))
      case "--okf-bundle" :: v :: rest if v.nonEmpty => loop(rest, acc.copy(okfBundleDir = v))
      case "--transport"  :: v :: rest if v.nonEmpty => loop(rest, acc.copy(transport = v))
      case "--rest-port"  :: v :: rest if v.nonEmpty =>
        v.toIntOption match {
          case Some(n) if n > 0 && n < 65536 => loop(rest, acc.copy(restPort = n))
          case _ => Left(s"--rest-port must be 1-65535, got '$v'")
        }
      case "--remote"     :: v :: rest if v.nonEmpty =>
        if (!v.startsWith("sc://"))
          Left(s"--remote must use the 'sc://' scheme, got: $v")
        else
          loop(rest, acc.copy(remote = Some(v)))
      case "--remote"     :: Nil => Left("--remote requires a value")
      case "--models"     :: Nil => Left("--models requires a value")
      case "--data"       :: Nil => Left("--data requires a value")
      case "--okf-bundle" :: Nil => Left("--okf-bundle requires a value")
      case "--transport"  :: Nil => Left("--transport requires a value")
      case "--rest-port"  :: Nil => Left("--rest-port requires a value")
      case "--remote"     :: Nil => Left("--remote requires a value")
      case other :: _ => Left(s"unknown argument: $other")
    }
    val init = Config(modelsDir = "", dataConfig = "", okfBundleDir = "",
                      transport = "stdio", restPort = 8080)
    loop(args.toList, init).flatMap { c =>
      if (c.modelsDir.isEmpty) Left("--models <dir> is required")
      else if (c.dataConfig.isEmpty) Left("--data <file> is required")
      else if (c.okfBundleDir.isEmpty) Left("--okf-bundle <dir> is required")
      else if (c.transport != "stdio" && c.transport != "rest")
        Left(s"--transport must be 'stdio' or 'rest', got '${c.transport}'")
      else Right(c)
    }
  }

  private val usage =
    """usage: semanticdf-mcp --models <dir> --data <file> --okf-bundle <dir> [options]
      |
      |  --models <dir>      directory of *.yml model files
      |  --data <file>       data-config YAML (see docs/agents/mcp-contract.md §"Server lifecycle")
      |  --okf-bundle <dir>  directory for OkfGen output; server caches the .md files
      |                      in memory at startup
      |  --transport {stdio,rest}
      |                      transport mode (default: stdio)
      |  --rest-port <N>     port for REST transport (default: 8080)
      |  --remote sc://host:port
      |                      Spark Connect URL. The MCP becomes a thin gRPC
      |                      client to an external Spark server. Requires
      |                      Spark 4.0+. Without this flag, the MCP runs
      |                      an in-process local Spark session (the default
      |                      since v0.1.x). Falls back to the
      |                      SEMANTICDF_SPARK_CONNECT_URL env var if the
      |                      flag is not set.
      |""".stripMargin
}
