package io.semanticdf.core.rel

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Phase 2 contract: prove `AggregateFn` is a usable, Spark-free
  * data record + the closed 16-variant enumeration. Per scala-data-
  * driven-refactor, this is pure data: the function is engine-
  * portable; the engine-specific compile is in the engine adapter.
  */
class AggregateFnSpec extends AnyFunSuite with Matchers {

  test("AggregateFn has exactly 16 cases") {
    val all: Set[AggregateFn] = Set(
      // 4 additive
      AggregateFn.Sum,
      AggregateFn.Count,
      AggregateFn.CountDistinct,
      AggregateFn.First,
      // 2 non-additive
      AggregateFn.Min,
      AggregateFn.Max,
      // 5 algebraic
      AggregateFn.Avg,
      AggregateFn.StddevSample,
      AggregateFn.StddevPopulation,
      AggregateFn.VarianceSample,
      AggregateFn.VariancePopulation,
      // 4 order-statistic
      AggregateFn.Median,
      AggregateFn.PercentileContinuous,
      AggregateFn.PercentileDiscrete,
      AggregateFn.ApproxPercentile,
      // 1 position
      AggregateFn.Last,
    )
    all.size shouldBe 16
  }

  test("each case is a singleton") {
    AggregateFn.Sum shouldBe AggregateFn.Sum
    AggregateFn.Median shouldBe AggregateFn.Median
    AggregateFn.Last shouldBe AggregateFn.Last
  }

  test("Sealed exhaustiveness: pattern-match over all 16 cases") {
    val all: Seq[AggregateFn] = Seq(
      AggregateFn.Sum,
      AggregateFn.Count,
      AggregateFn.CountDistinct,
      AggregateFn.Avg,
      AggregateFn.Min,
      AggregateFn.Max,
      AggregateFn.StddevSample,
      AggregateFn.StddevPopulation,
      AggregateFn.VarianceSample,
      AggregateFn.VariancePopulation,
      AggregateFn.Median,
      AggregateFn.PercentileContinuous,
      AggregateFn.PercentileDiscrete,
      AggregateFn.ApproxPercentile,
      AggregateFn.First,
      AggregateFn.Last,
    )
    all.foreach {
      case AggregateFn.Sum                  => ()
      case AggregateFn.Count                => ()
      case AggregateFn.CountDistinct        => ()
      case AggregateFn.Avg                  => ()
      case AggregateFn.Min                  => ()
      case AggregateFn.Max                  => ()
      case AggregateFn.StddevSample         => ()
      case AggregateFn.StddevPopulation     => ()
      case AggregateFn.VarianceSample       => ()
      case AggregateFn.VariancePopulation   => ()
      case AggregateFn.Median               => ()
      case AggregateFn.PercentileContinuous => ()
      case AggregateFn.PercentileDiscrete   => ()
      case AggregateFn.ApproxPercentile     => ()
      case AggregateFn.First                => ()
      case AggregateFn.Last                 => ()
    }
  }

  test("AggregateFn round-trips through Java serialization") {
    val cases: Seq[AggregateFn] = Seq(
      AggregateFn.Sum, AggregateFn.Count, AggregateFn.CountDistinct, AggregateFn.Avg,
      AggregateFn.Min, AggregateFn.Max,
      AggregateFn.StddevSample, AggregateFn.StddevPopulation,
      AggregateFn.VarianceSample, AggregateFn.VariancePopulation,
      AggregateFn.Median,
      AggregateFn.PercentileContinuous, AggregateFn.PercentileDiscrete,
      AggregateFn.ApproxPercentile,
      AggregateFn.First, AggregateFn.Last,
    )
    cases.foreach { v =>
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(v)
      oos.close()
      val bis = new java.io.ByteArrayInputStream(bos.toByteArray)
      val ois = new java.io.ObjectInputStream(bis)
      val restored = ois.readObject().asInstanceOf[AggregateFn]
      restored shouldBe v
    }
  }
}