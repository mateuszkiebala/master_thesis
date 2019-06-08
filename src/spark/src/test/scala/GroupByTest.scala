import com.holdenkarau.spark.testing.SharedSparkContext
import minimal_algorithms.examples.statistics_aggregators.{AvgAggregator, MaxAggregator, MinAggregator, SumAggregator}
import minimal_algorithms.MinimalAlgFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class TestGroupBy(key: Int, weight: Double) extends Serializable {
  def getKey: Int = this.key
  def getWeight: Double = this.weight
}

class GroupByTest extends FunSuite with SharedSparkContext with Matchers {
  val cmpKey = (o: TestGroupBy) => o.getKey

  def createRDD(spark: SparkSession): RDD[TestGroupBy] = {
    spark.sparkContext.parallelize(
      Array((1, 2), (1, 5), (1, -10), (2, 1), (10, 2), (5, 1), (10, 12), (2, 10), (10, -7), (5, 2), (10, 5)
      ).map{e => new TestGroupBy(e._1, e._2)})
  }

  test("GroupBy sum") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val statsAgg = (o: TestGroupBy) => new SumAggregator(o.getWeight)
    val result = new MinimalAlgFactory(spark, 2, createRDD(spark)).groupBy(cmpKey, statsAgg)
    assert(Set((1, -3.0), (2, 11.0), (5, 3.0), (10, 12.0)) == result.collect().map{case(k, v) => (k, v.getValue)}.toSet)
  }

  test("GroupBy min") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val statsAgg = (o: TestGroupBy) => new MinAggregator(o.getWeight)
    val result = new MinimalAlgFactory(spark, 2, createRDD(spark), 11).groupBy(cmpKey, statsAgg)
    assert(Set((1, -10.0), (2, 1.0), (5, 1.0), (10, -7.0)) == result.collect().map{case(k, v) => (k, v.getValue)}.toSet)
  }

  test("GroupBy max") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val statsAgg = (o: TestGroupBy) => new MaxAggregator(o.getWeight)
    val result = new MinimalAlgFactory(spark, 2, createRDD(spark), 11).groupBy(cmpKey, statsAgg)
    assert(Set((1, 5.0), (2, 10.0), (5, 2.0), (10, 12.0)) == result.collect().map{case(k, v) => (k, v.getValue)}.toSet)
  }

  test("GroupBy average") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val statsAgg = (o: TestGroupBy) => new AvgAggregator(o.getWeight, 1)
    val result = new MinimalAlgFactory(spark, 2, createRDD(spark)).groupBy(cmpKey, statsAgg)
    assert(Set((1, -1.0), (2, 5.5), (5, 1.5), (10, 3.0)) == result.collect().map{case(k, v) => (k, v.getValue)}.toSet)
  }
}
