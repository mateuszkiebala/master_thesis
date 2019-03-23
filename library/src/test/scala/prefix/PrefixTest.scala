package prefix

import minimal_algorithms.statistics_aggregators.{MaxAggregator, MinAggregator, SumAggregator}
import minimal_algorithms.statistics_aggregators.SumImplicits._
import minimal_algorithms.{MinimalAlgorithm, StatisticsMinimalAlgorithm}
import minimal_algorithms.prefix.SumPrefixSMAO
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class PrefixTest extends FunSuite with SharedSparkContext with Matchers {
  val elements = Seq(new SumPrefixSMAO(1), new SumPrefixSMAO(5), new SumPrefixSMAO(-10), new SumPrefixSMAO(1), new SumPrefixSMAO(2),
    new SumPrefixSMAO(1), new SumPrefixSMAO(12), new SumPrefixSMAO(10), new SumPrefixSMAO(-7), new SumPrefixSMAO(2))
  val rdd = spark.sparkContext.parallelize(elements)

  test("Prefix sum") {
      // when
    val minimalAlgorithm = new StatisticsMinimalAlgorithm[SumAggregator, SumPrefixSMAO](spark, 2)
    val result = minimalAlgorithm.importObjects(rdd).computePrefix.collect()

      // then
    //val expected = Array(-10, -17, -16, -15, -14, -12, -10, -5, 5, 17)
    //assert(expected sameElements result.map(o => o._1.getValue))
  }

  /*test("Prefix min") {
      // when
    val minimalAlgorithm = new MinimalAlgorithm[SumPrefixSMAO](spark, 2)
    val result = minimalAlgorithm.importObjects(rdd).computePrefix(new MinAggregation).collect()

      // then
    val expected = Array(-10, -10, -10, -10, -10, -10, -10, -10, -10, -10)
    assert(expected sameElements result.map(o => o._1))
  }

  test("Prefix max") {
      // when
    val minimalAlgorithm = new MinimalAlgorithm[SumPrefixSMAO](spark, 2)
    val result = minimalAlgorithm.importObjects(rdd).computePrefix(new MaxAggregation).collect()

      // then
    val expected = Array(-10, -7, 1, 1, 1, 2, 2, 5, 10, 12)
    assert(expected sameElements result.map(o => o._1))
  }*/
}
