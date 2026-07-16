package io.semanticdf.mcp

import io.modelcontextprotocol.json.McpJsonDefaults
import org.apache.spark.sql.SparkSession
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

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("semanticdf-mcp")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.ansi.enabled", "false")  // match library test baseline
      .getOrCreate()

    // Register the shutdown hook IMMEDIATELY after spark is created so that
    // any throw between here and the try block still triggers spark.stop().
    // Without this, an exception during `DataConfig.fromFile` or
    // `Models.load` (e.g. malformed YAML) would leak the Spark session —
    // Spark's default cleanup only runs on JVM exit, not on early throws.
    // The `server` field is a `var` so this hook can also call `server.close()`
    // once it's been built inside the try block (see assignment below).
    // SparkSession.stop() is idempotent so the hook is safe to invoke twice
    // (once after the build, once on JVM exit) — but we guard with a flag
    // to keep the intent explicit.
    @volatile var server: io.modelcontextprotocol.server.McpSyncServer = null
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      try {
        if (server != null) server.close()
      } finally {
        try { spark.stop() } catch { case _: Throwable => /* best-effort */ }
      }
    }))

    try {
      val dataConfig = DataConfig.fromFile(parsed.dataConfig)
      val models     = Models.load(parsed.modelsDir, dataConfig, spark)

      // Run OkfGen at startup, then read each *.md into memory. The server
      // sees the cache as `Map[modelName, markdown]` with O(1) lookup at
      // request time. See `OkfCache` scaladoc.
      val okf = OkfCache.build(parsed.modelsDir, parsed.okfBundleDir)

      val mapper = McpJsonDefaults.getMapper()
      server = Server.build(models, okf, spark, mapper)

      // Block until stdio closes (parent process exits / sends EOF) or
      // SIGINT/SIGTERM. The shutdown hook (registered above, BEFORE the try
      // block, so it fires on early throws) is responsible for calling
      // server.close() and spark.stop(). McpSyncServer's close() is idempotent.
      log.info("semanticdf-mcp listening on stdio. Press Ctrl-D to stop.")

      // Park the main thread — the SDK runs on a daemon thread pool and
      // close() is invoked from the shutdown hook above.
      Thread.currentThread().join()
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

  private case class Config(modelsDir: String, dataConfig: String, okfBundleDir: String)

  private def parseArgs(args: Seq[String]): Either[String, Config] = {
    @scala.annotation.tailrec
    def loop(it: List[String], acc: Config): Either[String, Config] = it match {
      case Nil => Right(acc)
      case "--models"     :: v :: rest if v.nonEmpty => loop(rest, acc.copy(modelsDir = v))
      case "--data"       :: v :: rest if v.nonEmpty => loop(rest, acc.copy(dataConfig = v))
      case "--okf-bundle" :: v :: rest if v.nonEmpty => loop(rest, acc.copy(okfBundleDir = v))
      case "--models"     :: Nil => Left("--models requires a value")
      case "--data"       :: Nil => Left("--data requires a value")
      case "--okf-bundle" :: Nil => Left("--okf-bundle requires a value")
      case other :: _ => Left(s"unknown argument: $other")
    }
    val init = Config(modelsDir = "", dataConfig = "", okfBundleDir = "")
    loop(args.toList, init).flatMap { c =>
      if (c.modelsDir.isEmpty) Left("--models <dir> is required")
      else if (c.dataConfig.isEmpty) Left("--data <file> is required")
      else if (c.okfBundleDir.isEmpty) Left("--okf-bundle <dir> is required")
      else Right(c)
    }
  }

  private val usage =
    """usage: semanticdf-mcp --models <dir> --data <file> --okf-bundle <dir>
      |
      |  --models <dir>     directory of *.yml model files
      |  --data <file>      data-config YAML (see docs/agents/mcp-contract.md §"Server lifecycle")
      |  --okf-bundle <dir> directory for OkfGen output; server caches the .md files
      |                     in memory at startup
      |""".stripMargin
}
