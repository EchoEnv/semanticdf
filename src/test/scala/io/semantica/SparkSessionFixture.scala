package io.semantica

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** In-memory `SparkSession` with correct lifecycle (DESIGN Phase 0, risk C1).
  *
  * `beforeAll` creates a local session; `afterAll` stops it. A leaked (un-`stop()`-ed)
  * session leaks driver memory and daemon threads across test runs — the classic
  * Spark dev-velocity tax. This trait is the one place that lifecycle is owned.
  */
trait SparkSessionFixture extends BeforeAndAfterAll { this: Suite =>

  @transient private var _spark: SparkSession = _

  protected def spark: SparkSession = _spark

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder()
      .master("local[2]")
      .appName("semantica-tests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.session.timeZone", "UTC")
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
