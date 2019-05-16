import minimal_algorithms.MinimalAlgorithm
import minimal_algorithms.examples.statistics_aggregators.{MaxAggregator, MinAggregator, SumAggregator}
import org.apache.spark.rdd.RDD
import org.scalatest.{FunSuite, Matchers}

class TestPrefixObject(weight: Double) extends Serializable {
  override def toString: String = "Weight: " + this.weight
  def getWeight: Double = this.weight
}

class PrefixTest extends FunSuite with SharedSparkContext with Matchers {
  val rdd: RDD[TestPrefixObject] = spark.sparkContext.parallelize(Seq(1, 5, -10, 1, 2, 1, 12, 10, -7, 2).map{e => new TestPrefixObject(e)})
  val minimalAlgorithm = new MinimalAlgorithm(spark, 2)
  val cmpKey = (o: TestPrefixObject) => o.getWeight

  test("Prefix sum") {
      // given
    val statsAgg = (o: TestPrefixObject) => new SumAggregator(o.getWeight)

      // when
    val result = minimalAlgorithm.prefix(rdd, cmpKey, statsAgg).collect()

      // then
    val expected = Array(-10, -17, -16, -15, -14, -12, -10, -5, 5, 17)
    assert(expected sameElements result.map(o => o._1.getValue))
  }

  test("Prefix min") {
      // given
    val statsAgg = (o: TestPrefixObject) => new MinAggregator(o.getWeight)

      // when
    val result = minimalAlgorithm.prefix(rdd, cmpKey, statsAgg).collect()

      // then
    val expected = Array(-10, -10, -10, -10, -10, -10, -10, -10, -10, -10)
    assert(expected sameElements result.map(o => o._1.getValue))
  }

  test("Prefix max") {
      // given
    val statsAgg = (o: TestPrefixObject) => new MaxAggregator(o.getWeight)

      // when
    val result = minimalAlgorithm.prefix(rdd, cmpKey, statsAgg).collect()

      // then
    val expected = Array(-10, -7, 1, 1, 1, 2, 2, 5, 10, 12)
    assert(expected sameElements result.map(o => o._1.getValue))
  }
}
