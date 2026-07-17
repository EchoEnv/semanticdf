package io.semanticdf.mcp

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** Shared SparkSession lifecycle for the MCP server test suite.
  *
  * All MCP test specs that need a SparkSession should mix in this trait.
  * It creates ONE `SparkSession` per test JVM (not per spec), so specs
  * can run in any order without one spec stopping another's session.
  *
  * == Why shared, not per-spec ==
  *
  * Previously each spec that used Spark created its own session in its
  * constructor and called `spark.stop()` in `afterAll`. When multiple
  * specs ran in the same JVM (which Scalatest does by default), the
  * last-`stopped` spec killed the shared `SparkContext` and caused
  * `'Cannot call methods on a stopped SparkContext'` errors in
  * sibling specs whose tests ran afterward.
  *
  * With `SparkFixture`, the spark is created once (on first use) and
  * never explicitly stopped — the JVM exit hook handles cleanup. This
  * matches the pattern used by `ListModelsSpec` (the only spec that
  * already did it right).
  *
  * Local to this module because test-source dependencies don't flow
  * between sibling Maven projects. */
trait SparkFixture extends BeforeAndAfterAll { this: Suite =>

  // Lazy: spark is created on first access, not at construction time.
  // This avoids spinning up the session for suites that never touch it.
  //
  // Made a stable `val` (not `def`) so callers can write
  // `import spark.implicits._` (which requires a stable identifier).
  @transient private lazy val _spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("semanticdf-mcp-tests")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  /** The shared SparkSession. Accessed as a `val` (not `def`) so callers
    * can use `import spark.implicits._`. Lazy — created on first access. */
  protected val spark: SparkSession = _spark
}