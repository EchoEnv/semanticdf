package io.semanticdf

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** In-memory `SparkSession` with correct lifecycle (DESIGN Phase 0, risk C1).
  *
  * `beforeAll` creates a local session; `afterAll` stops it. A leaked (un-`stop()`-ed)
  * session leaks driver memory and daemon threads across test runs — the classic
  * Spark dev-velocity tax. This trait is the one place that lifecycle is owned.
  *
  * `spark.sql.ansi.enabled=false` is set unconditionally: Spark 4.x enables ANSI SQL
  * mode by default (division by zero throws, not null). Tests use the Spark 3
  * semantics (null on div-by-zero) as the consistent cross-version baseline.
  * Production deployments on Spark 4 with ANSI mode enabled will get the stricter
  * exception — [[CalcHelpers$.safeDivide]] guards against it.
  */
trait SparkSessionFixture extends BeforeAndAfterAll { this: Suite =>

  @transient private var _spark: SparkSession = _

  protected def spark: SparkSession = _spark

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder()
      .master("local[2]")
      .appName("semanticdf-tests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.ansi.enabled", "false")  // consistent null-on-div-zero across 3.x and 4.x
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    try {
      if (_spark != null) _spark.stop()
    } finally {
      _spark = null
      super.afterAll()
    }
  }
}
