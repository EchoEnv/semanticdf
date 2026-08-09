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
        System.err.println(s"semanticdf-mcp: ${err.message}")
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

      // Construct the engine registry (per design §6.4). For v1,
      // we register only the Spark engine provider (Trino and other
      // providers are future work). The default is "spark" so existing
      // clients get the legacy-equivalent path (which the spark engine
      // provider implements).
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

  private[mcp] case class Config(
      modelsDir: String,
      dataConfig: String,
      okfBundleDir: String,
      transport: String,
      restPort: Int,
      /** Spark Connect URL (None → local). The MCP becomes a thin
        * gRPC client to an external Spark Connect server when set. */
      remote: Option[String] = None,
  )

  // -------------------------------------------------------------------
  // Typed CLI parse errors (per docs/design/error-handling-style.md).
  //
  // Per the standard's hard bans:
  //   - No `Either[String, X]` in any code path.
  //   - All sealed error ADTs use SPECIFIC cases (no generic `ParseError`).
  //
  // Each case carries the data needed to format a stable, programmatic
  // human-readable message via `.message`. Mirrors the CLI consumer's
  // `CliParseError` pattern (PR #433) and the adapter parse-helper
  // pattern (PR #434).
  //
  // Made `private[mcp]` so the test spec (in the same package) can
  // assert on the cases directly — per scala-impact-analysis, the
  // blast radius is minimal (test surface only).
  // -------------------------------------------------------------------
  sealed trait McpParseError extends Product with Serializable {
    /** Stable human-readable message for stderr / logs. */
    def message: String
  }
  object McpParseError {
    final case class MissingFlagValue(flag: String) extends McpParseError {
      val message: String = s"$flag requires a value"
    }
    final case class MissingRequiredArgument(flag: String, usage: String) extends McpParseError {
      val message: String = s"$flag $usage is required"
    }
    final case class InvalidIntRange(flag: String, value: String, min: Int, max: Int)
        extends McpParseError {
      val message: String = s"$flag must be $min-$max, got '$value'"
    }
    final case class InvalidScheme(flag: String, value: String, expectedScheme: String)
        extends McpParseError {
      val message: String = s"$flag must use the '$expectedScheme' scheme, got: $value"
    }
    final case class InvalidEnumValue(flag: String, value: String, allowedValues: Set[String])
        extends McpParseError {
      val message: String =
        s"$flag must be one of [${allowedValues.mkString(", ")}], got '$value'"
    }
    final case class UnknownArgument(arg: String) extends McpParseError {
      val message: String = s"unknown argument: $arg"
    }
  }

  private[mcp] def parseArgs(args: Seq[String]): Either[McpParseError, Config] = {
    @scala.annotation.tailrec
    def loop(it: List[String], acc: Config): Either[McpParseError, Config] = it match {
      case Nil => Right(acc)
      case "--models"     :: v :: rest if v.nonEmpty => loop(rest, acc.copy(modelsDir = v))
      case "--data"       :: v :: rest if v.nonEmpty => loop(rest, acc.copy(dataConfig = v))
      case "--okf-bundle" :: v :: rest if v.nonEmpty => loop(rest, acc.copy(okfBundleDir = v))
      case "--transport"  :: v :: rest if v.nonEmpty => loop(rest, acc.copy(transport = v))
      case "--rest-port"  :: v :: rest if v.nonEmpty =>
        v.toIntOption match {
          case Some(n) if n > 0 && n < 65536 => loop(rest, acc.copy(restPort = n))
          case _ => Left(McpParseError.InvalidIntRange(
            flag = "--rest-port", value = v, min = 1, max = 65535
          ))
        }
      case "--remote"     :: v :: rest if v.nonEmpty =>
        if (!v.startsWith("sc://"))
          Left(McpParseError.InvalidScheme(
            flag = "--remote", value = v, expectedScheme = "sc://"
          ))
        else
          loop(rest, acc.copy(remote = Some(v)))
      case ("--remote"     :: Nil) => Left(McpParseError.MissingFlagValue(flag = "--remote"))
      case ("--models"     :: Nil) => Left(McpParseError.MissingFlagValue(flag = "--models"))
      case ("--data"       :: Nil) => Left(McpParseError.MissingFlagValue(flag = "--data"))
      case ("--okf-bundle" :: Nil) => Left(McpParseError.MissingFlagValue(flag = "--okf-bundle"))
      case ("--transport"  :: Nil) => Left(McpParseError.MissingFlagValue(flag = "--transport"))
      case ("--rest-port"  :: Nil) => Left(McpParseError.MissingFlagValue(flag = "--rest-port"))
      case other :: _ => Left(McpParseError.UnknownArgument(arg = other))
    }
    val init = Config(modelsDir = "", dataConfig = "", okfBundleDir = "",
                      transport = "stdio", restPort = 8080)
    loop(args.toList, init).flatMap { c =>
      if (c.modelsDir.isEmpty)
        Left(McpParseError.MissingRequiredArgument(flag = "--models", usage = "<dir>"))
      else if (c.dataConfig.isEmpty)
        Left(McpParseError.MissingRequiredArgument(flag = "--data", usage = "<file>"))
      else if (c.okfBundleDir.isEmpty)
        Left(McpParseError.MissingRequiredArgument(flag = "--okf-bundle", usage = "<dir>"))
      else if (c.transport != "stdio" && c.transport != "rest")
        Left(McpParseError.InvalidEnumValue(
          flag = "--transport",
          value = c.transport,
          allowedValues = Set("stdio", "rest"),
        ))
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
