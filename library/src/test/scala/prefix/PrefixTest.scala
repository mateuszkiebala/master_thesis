package prefix

import minimal_algorithms.statistics_aggregators.{MaxAggregator, MinAggregator, SumAggregator}
import minimal_algorithms.StatisticsMinimalAlgorithm
import minimal_algorithms.examples.prefix.{MaxPrefixSMAO, MinPrefixSMAO, SumPrefixSMAO}
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class PrefixTest extends FunSuite with SharedSparkContext with Matchers {
  val elements = Seq(1, 5, -10, 1, 2, 1, 12, 10, -7, 2)

  test("Prefix sum") {
      // when
    val minimalAlgorithm = new StatisticsMinimalAlgorithm[SumPrefixSMAO](spark, 2)
    val rdd = spark.sparkContext.parallelize(elements.map{e => new SumPrefixSMAO(e)})
    val result = minimalAlgorithm.importObjects(rdd).computePrefix.collect()

      // then
    val expected = Array(-10, -17, -16, -15, -14, -12, -10, -5, 5, 17)
    assert(expected sameElements result.map(o => o._1.asInstanceOf[SumAggregator].getValue))
  }

  test("Prefix min") {
      // when
    val minimalAlgorithm = new StatisticsMinimalAlgorithm[MinPrefixSMAO](spark, 2)
    val rdd = spark.sparkContext.parallelize(elements.map{e => new MinPrefixSMAO(e)})
    val result = minimalAlgorithm.importObjects(rdd).computePrefix.collect()

      // then
    val expected = Array(-10, -10, -10, -10, -10, -10, -10, -10, -10, -10)
    assert(expected sameElements result.map(o => o._1.asInstanceOf[MinAggregator].getValue))
  }

  test("Prefix max") {
      // when
    val minimalAlgorithm = new StatisticsMinimalAlgorithm[MaxPrefixSMAO](spark, 2)
    val rdd = spark.sparkContext.parallelize(elements.map{e => new MaxPrefixSMAO(e)})
    val result = minimalAlgorithm.importObjects(rdd).computePrefix.collect()

      // then
    val expected = Array(-10, -7, 1, 1, 1, 2, 2, 5, 10, 12)
    assert(expected sameElements result.map(o => o._1.asInstanceOf[MaxAggregator].getValue))
  }
}
