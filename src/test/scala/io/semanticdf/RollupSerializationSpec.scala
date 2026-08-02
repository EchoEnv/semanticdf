package io.semanticdf

import org.apache.spark.sql.functions.sum
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.semanticdf.rollup.{
  Rollup, RollupMeasure, RollupAggregator,
  RollupFreshness, OnStalePolicy,
}

/** Java-serialization round-trip tests for the rollup package.
  *
  * Background: the round-4 stacked-lens review of the v0.2.4 rollup
  * feature surfaced that the existing `SerializationSpec` covered
  * library types generally (SemanticTable, Predicate, SortKey, ...)
  * but had NO tests for any rollup-package types. The design intent
  * ("pure metadata → Java-serializable so a SemanticTable with
  * `.withRollup(...)` can ship across executors") was untested.
  *
  * This spec closes that gap. It round-trips each rollup type
  * through Java serialization and asserts identity is preserved.
  *
  * == What is and isn't tested ==
  *
  * Tested (pure-metadata round-trip):
  *   - `Rollup` with default `Track` freshness (5-arg constructor)
  *   - `Rollup` with `RollupFreshness.NoTracking` (6-arg constructor)
  *   - `RollupMeasure` (sum and count)
  *   - `RollupAggregator.Sum` / `RollupAggregator.Count`
  *   - `OnStalePolicy.FallBackToBase` / `OnStalePolicy.Error`
  *   - `SemanticTable` carrying a rollup (the user's cluster-mode case)
  *
  * Not tested (documented limitations, not part of the contract):
  *   - `RollupRegistry` (holds `() => DataFrame` thunks — non-Serializable
  *     in practice even though `extends Serializable`. Documented.)
  *   - `RollupQuery` (driver-local query executor; intentionally does
  *     NOT extend Serializable.)
  *   - User-supplied custom `watermarkProvider` closures on the 6-arg
  *     `Rollup.apply` overload — caller-controlled, may capture outer
  *     state, not part of the library contract.
  *
  * == Failure mode ==
  *
  * A regression here is a hard cluster-mode break: a user capturing
  * a `SemanticTable.withRollup(...)` in a closure (broadcast variable,
  * accumulator, or UDF) would see `NotSerializableException` at job
  * submission time. The tests below catch this at the test bench.
  */
class RollupSerializationSpec extends AnyFunSuite with SparkSessionFixture with FlightsFixture {

  // -- helpers --

  private def roundTrip[T](obj: T): T = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  // -- Provider thunk for tests --
  //
  // The `Rollup.apply` constructor takes a `sourceProvider: () => DataFrame`
  // thunk. We supply one that returns a small hand-built DataFrame matching
  // the rollup's expected schema. The round-trip test discards the provider
  // after construction (only the resulting metadata is serialized) so the
  // DataFrame is never sent over the wire.

  private def rollupDfFor(spark: org.apache.spark.sql.SparkSession) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        org.apache.spark.sql.Row("AA", 100L),
        org.apache.spark.sql.Row("UA", 200L),
        org.apache.spark.sql.Row("DL", 300L),
      )),
      org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("carrier", org.apache.spark.sql.types.StringType),
        org.apache.spark.sql.types.StructField("pax_sum", org.apache.spark.sql.types.LongType),
      ))
    )

  // -- tests: rollup primitives --

  test("Rollup with default freshness: Java-serializable and identity-preserving") {
    // 5-arg constructor: applies a default `Track(watermarkProvider = () => Instant.now, ...)`.
    // This is the canonical use path. Serialization must NOT touch the supplied
    // `sourceProvider` thunk (only metadata survives; the caller registers the
    // same provider with a `RollupRegistry` at use-time).
    val rollup = Rollup(
      name             = "flights_by_carrier",
      baseModel        = "flights",
      rollupDimensions = Seq("carrier"),
      rollupMeasures   = Seq(RollupMeasure("pax_sum", "sum", "pax_sum")),
      sourceProvider   = () => rollupDfFor(spark),
    )
    val round = roundTrip(rollup)
    assert(round.name == rollup.name, s"name round-trip: ${round.name}")
    assert(round.baseModel == rollup.baseModel, s"baseModel round-trip: ${round.baseModel}")
    assert(round.rollupDimensions == rollup.rollupDimensions,
      s"dims: ${round.rollupDimensions} vs ${rollup.rollupDimensions}")
    assert(round.rollupMeasures.size == 1 &&
           round.rollupMeasures.head.name == rollup.rollupMeasures.head.name &&
           round.rollupMeasures.head.storageCol == rollup.rollupMeasures.head.storageCol,
      s"measure round-trip: ${round.rollupMeasures}")
    assert(round.precomputedRowCount == 3L,
      s"precomputedRowCount: ${round.precomputedRowCount}")
    assert(round.precomputedColumns == Set("carrier", "pax_sum"),
      s"precomputedColumns: ${round.precomputedColumns}")
  }

  test("Rollup with NoTracking freshness: Java-serializable") {
    // 6-arg constructor with explicit NoTracking. Common for batch rollups
    // where staleness is acceptable.
    val rollup = Rollup(
      name             = "flights_by_carrier_batch",
      baseModel        = "flights",
      rollupDimensions = Seq("carrier"),
      rollupMeasures   = Seq(RollupMeasure("pax_sum", "sum", "pax_sum")),
      sourceProvider   = () => rollupDfFor(spark),
      freshness        = RollupFreshness.NoTracking,
    )
    val round = roundTrip(rollup)
    assert(round.freshness == RollupFreshness.NoTracking,
      s"freshness should be NoTracking: ${round.freshness}")
    // All other fields must match too.
    assert(round.name == "flights_by_carrier_batch")
    assert(round.precomputedRowCount == 3L)
  }

  test("RollupMeasure: Java-serializable (string + sealed-ADT aggregator)") {
    val measure = RollupMeasure("pax_sum", "sum", "pax_sum")
    val round = roundTrip(measure)
    assert(round.name == "pax_sum")
    assert(round.storageCol == "pax_sum")
    assert(round.aggregator == RollupAggregator.Sum,
      s"aggregator should be Sum (case-object singleton): got ${round.aggregator}")
  }

  // -- tests: sealed-ADT singletons --

  test("RollupAggregator.Sum: Java-serializable as case-object singleton") {
    val agg = RollupAggregator.Sum
    val round = roundTrip(agg)
    assert(round == RollupAggregator.Sum,
      "RollupAggregator.Sum should round-trip as the singleton itself")
  }

  test("RollupAggregator.Count: Java-serializable as case-object singleton") {
    val agg = RollupAggregator.Count
    val round = roundTrip(agg)
    assert(round == RollupAggregator.Count,
      "RollupAggregator.Count should round-trip as the singleton itself")
  }

  test("OnStalePolicy singletons: Java-serializable") {
    // The round-trip of a case-object singleton yields the same reference.
    val fb = OnStalePolicy.FallBackToBase
    val er = OnStalePolicy.Error
    assert(roundTrip(fb) eq fb, "FallBackToBase should be a reference-preserving singleton")
    assert(roundTrip(er) eq er, "Error should be a reference-preserving singleton")
  }

  // -- tests: end-to-end — the user's actual cluster-mode use case --

  test("SemanticTable.withRollup(...): rollup metadata survives Java round-trip") {
    // The load-bearing test: a `SemanticTable` carrying a rollup, captured
    // in a closure (broadcast, accumulator, UDF), must round-trip through
    // Java serialization in cluster mode.
    val rollup = Rollup(
      name             = "flights_by_carrier",
      baseModel        = "flights",
      rollupDimensions = Seq("carrier"),
      rollupMeasures   = Seq(RollupMeasure("pax_sum", "sum", "pax_sum")),
      sourceProvider   = () => rollupDfFor(spark),
    )
    val model = io.semanticdf.toSemanticTable(flightsDf, name = Some("flights"))
      .withDimensions(io.semanticdf.Dimension("carrier", t => t("carrier")))
      .withMeasures(io.semanticdf.Measure("pax_sum", t => sum(t("pax"))))
      .withRollup(rollup)

    val round = roundTrip(model)
    assert(round.name == model.name)
    assert(round.listRollups().size == 1, s"should carry 1 rollup, got ${round.listRollups().size}")
    val roundRollup = round.listRollups().head
    assert(roundRollup.name == "flights_by_carrier")
    assert(roundRollup.baseModel == "flights")
    assert(roundRollup.rollupDimensions == Seq("carrier"))
    assert(roundRollup.precomputedRowCount == 3L)
  }

  test("SemanticTable: listRollups() returns equal content after round-trip (regression guard)") {
    // Lower-overhead variant of the previous test: focuses on the `rollups`
    // field — the new field added in PR #330 that wasn't covered by the
    // pre-existing `SemanticTable: Java-serializable` test.
    val r1 = Rollup("r1", "flights", Seq("carrier"),
      Seq(RollupMeasure("pax_sum", "sum", "pax_sum")),
      () => rollupDfFor(spark))
    val r2 = Rollup("r2", "flights", Seq("carrier"),
      Seq(RollupMeasure("pax_sum", "count", "pax_sum")),
      () => rollupDfFor(spark))
    val model = io.semanticdf.toSemanticTable(flightsDf, name = Some("flights"))
      .withRollup(r1)
      .withRollup(r2)

    val round = roundTrip(model)
    val roundRollups = round.listRollups()
    assert(roundRollups.size == 2, s"both rollups should round-trip, got ${roundRollups.size}")
    assert(roundRollups.map(_.name).toSet == Set("r1", "r2"),
      s"rollup names: ${roundRollups.map(_.name)}")
  }
}
