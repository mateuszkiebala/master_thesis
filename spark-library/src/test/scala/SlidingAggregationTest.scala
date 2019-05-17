import com.holdenkarau.spark.testing.SharedSparkContext
import minimal_algorithms.examples.statistics_aggregators.{AvgAggregator, MaxAggregator, MinAggregator, SumAggregator}
import minimal_algorithms.sliding_aggregation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class TestSlidingObject(key: Int, weight: Double) extends Serializable {
  override def toString: String = "Key: " + this.key + " | Weight: " + this.weight
  def getWeight: Double = this.weight
  def getKey: Int = this.key
}

class SlidingAggregationTest extends FunSuite with SharedSparkContext with Matchers {
  val cmpKey = (o: TestSlidingObject) => o.getKey

  def createRDD(spark: SparkSession): RDD[TestSlidingObject] = {
    spark.sparkContext.parallelize(
      Array((1, 2), (3, 5), (2, -10), (4, 1), (7, 2), (6, 1), (5, 12), (8, 10), (10, -7), (9, 2), (11, 5))
    ).map{e => new TestSlidingObject(e._1, e._2)}
  }

  test("SlidingAggregation sum") {
      // given
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val rdd = createRDD(spark)
    val statsAgg = (o: TestSlidingObject) => new SumAggregator(o.getWeight)

      // when
    val result = new MinimalSlidingAggregation(spark, 3).aggregate(rdd, 7, cmpKey, statsAgg).collect()

      // then
    val expected = Array((1, 2.0), (2, -8.0), (3, -3.0), (4, -2.0), (5, 10.0), (6, 11.0), (7, 13.0), (8, 21.0), (9, 33.0), (10, 21.0), (11, 25.0))
    assert(expected.sameElements(result.map{e => (e._1.getKey, e._2.getValue)}))
  }

  test("SlidingAggregation min") {
      // given
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val rdd = createRDD(spark)
    val statsAgg = (o: TestSlidingObject) => new MinAggregator(o.getWeight)

      // when
    val result = new MinimalSlidingAggregation(spark, 3).aggregate(rdd, 7, cmpKey, statsAgg, 11).collect()

      // then
    val expected = Array((1, 2.0), (2, -10.0), (3, -10.0), (4, -10.0), (5, -10), (6, -10.0), (7, -10.0), (8, -10.0), (9, 1.0), (10, -7.0), (11, -7.0))
    assert(expected.sameElements(result.map{e => (e._1.getKey, e._2.getValue)}))
  }

  test("SlidingAggregation max") {
      // given
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val rdd = createRDD(spark)
    val statsAgg = (o: TestSlidingObject) => new MaxAggregator(o.getWeight)

      // when
    val result = new MinimalSlidingAggregation(spark, 3).aggregate(rdd, 7, cmpKey, statsAgg).collect()

      // then
    val expected = Array((1, 2.0), (2, 2.0), (3, 5.0), (4, 5.0), (5, 12.0), (6, 12.0), (7, 12.0), (8, 12.0), (9, 12.0), (10, 12.0), (11, 12.0))
    assert(expected.sameElements(result.map{e => (e._1.getKey, e._2.getValue)}))
  }


  test("SlidingAggregation average") {
      // given
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val rdd = createRDD(spark)
    val statsAgg = (o: TestSlidingObject) => new AvgAggregator(o.getWeight, 1)

      // when
    val result = new MinimalSlidingAggregation(spark, 3).aggregate(rdd, 7, cmpKey, statsAgg, 11).collect()

      // then
    val expected = Array((1, 2.0), (2, -4.0), (3, -1.0), (4, -0.5), (5, 2.0), (6, 11.0 / 6), (7, 13.0 / 7), (8, 3.0), (9, 33.0 / 7), (10, 3.0), (11, 25.0 / 7))
    assert(expected.sameElements(result.map{e => (e._1.getKey, e._2.getValue)}))
  }
}
