package prefix

import minimal_algorithms.MinimalAlgorithm
import minimal_algorithms.examples.statistics_aggregators.{MaxAggregator, MinAggregator, SumAggregator}
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class TestPrefixObject(weight: Double) extends Serializable {
  override def toString: String = "Weight: " + this.weight
  def getWeight: Double = this.weight
}

class PrefixTest extends FunSuite with SharedSparkContext with Matchers {
  val rdd = spark.sparkContext.parallelize(Seq(1, 5, -10, 1, 2, 1, 12, 10, -7, 2).map{e => new TestPrefixObject(e)})
  val minimalAlgorithm = new MinimalAlgorithm[TestPrefixObject](spark, 2).importObjects(rdd)
  val cmpKey = (o: TestPrefixObject) => o.getWeight

  test("Prefix sum") {
      // given
    val statsAgg = (o: TestPrefixObject) => new SumAggregator(o.getWeight)

      // when
    val result = minimalAlgorithm.prefixed(cmpKey, statsAgg).collect()

      // then
    val expected = Array(-10, -17, -16, -15, -14, -12, -10, -5, 5, 17)
    assert(expected sameElements result.map(o => o._1.asInstanceOf[SumAggregator].getValue))
  }

  test("Prefix min") {
      // given
    val statsAgg = (o: TestPrefixObject) => new MinAggregator(o.getWeight)

      // when
    val result = minimalAlgorithm.prefixed(cmpKey, statsAgg).collect()

      // then
    val expected = Array(-10, -10, -10, -10, -10, -10, -10, -10, -10, -10)
    assert(expected sameElements result.map(o => o._1.asInstanceOf[MinAggregator].getValue))
  }

  test("Prefix max") {
      // given
    val statsAgg = (o: TestPrefixObject) => new MaxAggregator(o.getWeight)

      // when
    val result = minimalAlgorithm.prefixed(cmpKey, statsAgg).collect()

      // then
    val expected = Array(-10, -7, 1, 1, 1, 2, 2, 5, 10, 12)
    assert(expected sameElements result.map(o => o._1.asInstanceOf[MaxAggregator].getValue))
  }
}
