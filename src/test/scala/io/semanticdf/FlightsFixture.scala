package io.semanticdf

import org.apache.spark.sql.DataFrame

/** In-memory flights fixture using BSL `test_query.py`'s exact data.
  *
  * Self-contained (BSL's real fixture downloads a parquet from malloy-samples; we avoid
  * the network dependency). Phase 1's golden tests assert against these rows so that
  * parity with BSL is checkable: `{AA→550, UA→775, DL→1050}` for total passengers.
  */
trait FlightsFixture { this: SparkSessionFixture =>

  protected def flightsDf: DataFrame = {
    val session = spark
    import session.implicits._
    val carriers = (1 to 5).flatMap(_ => Seq("AA", "UA", "DL", "AA", "UA", "DL")).toSeq
    val origins  = (1 to 5).flatMap(_ => Seq("JFK", "SFO", "LAX", "ORD", "DEN", "ATL")).toSeq
    val distance = (1 to 5).flatMap(_ => Seq(100, 200, 300, 150, 250, 350)).toSeq
    val pax      = (1 to 5).flatMap(_ => Seq(50, 75, 100, 60, 80, 110)).toSeq
    carriers.zip(origins).zip(distance).zip(pax).map {
      case (((c, o), d), p) => FlightRow(c, o, d, p)
    }.toDF()
  }

  /** Phase 6 fixture: flights with a `ts` timestamp column spanning 3 months.
    *
    * 30 flights total: 10 in 2024-01, 10 in 2024-02, 10 in 2024-03. Each month has
    * the same per-carrier passenger split (AA=110, UA=155, DL=210 across the 6-flight
    * pattern ×... ). Per-month total = 475 passengers (50+75+100+60+80+110). */
  protected def flightsWithTimeDf: DataFrame = {
    val session = spark
    import session.implicits._
    val base = Seq(
      // (carrier, origin, distance, passengers)
      ("AA", "JFK", 100, 50),
      ("UA", "SFO", 200, 75),
      ("DL", "LAX", 300, 100),
      ("AA", "ORD", 150, 60),
      ("UA", "DEN", 250, 80),
      ("DL", "ATL", 350, 110),
    )
    // 3 months × the 6-row pattern = 18 rows (simpler than the 30 above, easier to reason).
    val months = Seq("2024-01-15", "2024-02-15", "2024-03-15")
    val rows = for {
      ts <- months
      (c, o, d, p) <- base
    } yield FlightWithTimeRow(java.sql.Timestamp.valueOf(ts + " 10:00:00"), c, o, d, p)
    rows.toDF()
  }

  // ---- Phase 4: join fixtures (orders / customers / line_items) ---------------

  protected def ordersDf: DataFrame = {
    val session = spark
    import session.implicits._
    Seq(
      // carrier → customer_id (one-to-many: one customer, many carriers)
      OrderRow("order_1", "cust_A", "AA", 1),
      OrderRow("order_2", "cust_A", "UA", 1),
      OrderRow("order_3", "cust_B", "DL", 1),
    ).toDF()
  }

  protected def customersDf: DataFrame = {
    val session = spark
    import session.implicits._
    Seq(
      CustomerRow("cust_A", "Alice", "NYC"),
      CustomerRow("cust_B", "Bob",   "LAX"),
    ).toDF()
  }

  protected def lineItemsDf: DataFrame = {
    val session = spark
    import session.implicits._
    Seq(
      // order_id → qty → price_cents. Total revenue = qty * price_cents.
      LineItemRow("order_1", 3, 100),  // 300
      LineItemRow("order_1", 2, 100),  // 200 → total order_1 = 500
      LineItemRow("order_2", 1, 300),  // 300 → total order_2 = 300
      LineItemRow("order_3", 4,  50),  // 200 → total order_3 = 200
    ).toDF()
  }
}

// Top-level (not nested) so Spark can derive an encoder for it.
private[semanticdf] case class FlightRow(
    carrier: String,
    origin: String,
    distance: Int,
    passengers: Int,
)

private[semanticdf] case class FlightWithTimeRow(
    ts: java.sql.Timestamp,
    carrier: String,
    origin: String,
    distance: Int,
    passengers: Int,
)

private[semanticdf] case class OrderRow(
    order_id: String,
    customer_id: String,
    carrier: String,
    qty: Int,
)

private[semanticdf] case class CustomerRow(
    customer_id: String,
    name: String,
    city: String,
)

private[semanticdf] case class LineItemRow(
    order_id: String,
    qty: Int,
    price_cents: Int,
)
