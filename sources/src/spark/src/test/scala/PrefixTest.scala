import com.holdenkarau.spark.testing.SharedSparkContext
import minimal_algorithms.spark.SparkMinAlgFactory
import minimal_algorithms.spark.examples.statistics_aggregators.{MaxAggregator, MinAggregator, SumAggregator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class TestPrefixObject(weight: Double) extends Serializable {
  override def toString: String = "Weight: " + this.weight
  def getWeight: Double = this.weight
}

class PrefixTest extends FunSuite with SharedSparkContext with Matchers {
  val cmpKey = (o: TestPrefixObject) => o.getWeight

  def createRDD(spark: SparkSession): RDD[TestPrefixObject] = {
    spark.sparkContext.parallelize(Seq(1, 5, -10, 1, 2, 1, 12, 10, -7, 2).map{e => new TestPrefixObject(e)})
  }

  test("Prefix sum") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val statsAgg = (o: TestPrefixObject) => new SumAggregator(o.getWeight)
    val result = new SparkMinAlgFactory(spark, 2).prefix(createRDD(spark), cmpKey, statsAgg).collect()
    val expected = Array(-10, -17, -16, -15, -14, -12, -10, -5, 5, 17)
    assert(expected sameElements result.map(o => o._1.getValue))
  }

  test("Prefix min") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val statsAgg = (o: TestPrefixObject) => new MinAggregator(o.getWeight)
    val result = new SparkMinAlgFactory(spark, 2).prefix(createRDD(spark), cmpKey, statsAgg).collect()
    val expected = Array(-10, -10, -10, -10, -10, -10, -10, -10, -10, -10)
    assert(expected sameElements result.map(o => o._1.getValue))
  }

  test("Prefix max") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val statsAgg = (o: TestPrefixObject) => new MaxAggregator(o.getWeight)
    val result = new SparkMinAlgFactory(spark, 2).prefix(createRDD(spark), cmpKey, statsAgg).collect()
    val expected = Array(-10, -7, 1, 1, 1, 2, 2, 5, 10, 12)
    assert(expected sameElements result.map(o => o._1.getValue))
  }
}
