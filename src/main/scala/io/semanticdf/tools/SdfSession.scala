package io.semanticdf.tools

import org.apache.spark.sql.SparkSession

/** Centralized session factory for the semanticdf tooling (CLI subcommands
  * and the MCP server).
  *
  * ==Why this exists==
  *
  * The CLI and MCP both used to call `SparkSession.builder().master("local[*]").getOrCreate()`
  * directly. That had two costs:
  *
  *   1. Every CLI invocation started a fresh in-process Spark JVM.
  *      For a `mvn exec:java ... query` that's ~3s of classloader / Spark
  *      init time on top of the JVM warm-up.
  *
  *   2. Every CLI invocation created its OWN session, ignoring any
  *      session the test (or the parent process) had already set up.
  *      In PR #209 the lineage CLI test hit this: the test's
  *      `SparkSessionFixture` registered a temp view, but the CLI's
  *      `getOrCreate` returned a different session and the view was
  *      invisible.
  *
  * With [[create]], the CLI/MCP use the test's existing session when
  * one is available (no `--remote` → reuse). With `--remote sc://...`
  * they connect to a Spark Connect server, which IS a different session
  * (intentional: the server owns the catalog and the JVM lifecycle).
  *
  * ==Design choice: plain `object` factory, not a trait==
  *
  * The two branches (local vs remote) only differ in one call:
  * `master("local[*]")` vs `remote("sc://...")`. There's no runtime
  * polymorphism we want to inject (no test-double, no alternate
  * lifecycle). A `trait` would be convention, not data — per the
  * clean-architecture skill's "earn your place" rule.
  *
  * ==Library impact: zero==
  *
  * The library takes `SparkSession` as a parameter everywhere. Connect
  * sessions satisfy the same interface. No library change needed.
  *
  * ==Tag cleanup contract==
  *
  * The library uses `addJobTag` (per the cancellation fix). The factory
  * does not manage tag lifecycle — that's the caller's responsibility
  * (see `Query.withTimeout`). The factory only handles session creation. */
object SdfSession {

  /** The env var name. Overridden by `--remote` on the CLI. */
  val RemoteUrlEnvVar: String = "SEMANTICDF_SPARK_CONNECT_URL"

  /** Create a `SparkSession`. Either:
    *
    *   - **Local mode** (default): `master("local[*]")`. The session is
    *     either a fresh in-process one or a `getOrCreate`-d existing
    *     one (e.g. a test session).
    *
    *   - **Connect mode** (when `remoteOpt` is `Some(url)`): a Spark
    *     Connect client. `url` must be the full `sc://host:port` form
    *     (the `sc://` scheme is the contract Spark exposes).
    *
    * @param appName  the Spark app name (visible in the Spark UI / logs)
    * @param remoteOpt  `Some(sc://...)` to connect to a remote server;
    *                   `None` for the default local mode
    */
  def create(appName: String, remoteOpt: Option[String]): SparkSession = {
    val b = SparkSession.builder()
      .appName(appName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.ansi.enabled", "false")  // match library test baseline
    val configured = remoteOpt match {
      case Some(url) =>
        require(url.startsWith("sc://"),
          s"Spark Connect URL must use the 'sc://' scheme, got: $url")
        // SparkSession.Builder.remote(String) was added in Spark 4.0.
        // On Spark 3.5.x the standard builder does NOT have it (Spark
        // Connect on 3.x is a separate artifact). We use reflection so
        // the code compiles on both versions, and give a clear error
        // message on 3.x.
        try {
          val remoteMethod = b.getClass.getMethod("remote", classOf[String])
          remoteMethod.invoke(b, url).asInstanceOf[org.apache.spark.sql.SparkSession.Builder]
        } catch {
          case _: NoSuchMethodException =>
            throw new UnsupportedOperationException(
              "Spark Connect mode requires Spark 4.0 or later. " +
              "On Spark 3.x, the Spark Connect API is a separate artifact; " +
              s"you're on ${org.apache.spark.SPARK_VERSION}. " +
              "Build with -Pspark4 (or set spark.version=4.x) to enable Connect mode.")
        }
      case None =>
        // Default local master. The test fixture (SparkSessionFixture)
        // uses 'local[2]', but getOrCreate reuses whatever the parent
        // process already configured — so the test session wins
        // when present.
        b.master("local[*]")
    }
    configured.getOrCreate()
  }

  /** Convenience: read the env var, then fall back to `None`. */
  def createFromEnv(appName: String, flagOverride: Option[String]): SparkSession = {
    val effective = flagOverride match {
      case Some(url) => Some(url)
      case None      => sys.env.get(RemoteUrlEnvVar)
    }
    create(appName, effective)
  }
}
