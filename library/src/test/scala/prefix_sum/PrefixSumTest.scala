package prefix_sum

import minimal_algorithms.{ExampleMao, MinimalAlgorithm}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class PrefixSumTest extends FunSuite {
  val spark = SparkSession.builder().appName("PrefixSumTest").master("local").getOrCreate()
  val elements = Seq(new ExampleMao(1), new ExampleMao(5), new ExampleMao(-10), new ExampleMao(1), new ExampleMao(2),
    new ExampleMao(1), new ExampleMao(12), new ExampleMao(10), new ExampleMao(-7), new ExampleMao(2))
  val rdd = spark.sqlContext.sparkContext.parallelize(elements)

  test("Prefix sum") {
      // when
    val minimalAlgorithm = new MinimalAlgorithm[ExampleMao](spark, 2)
    val result = minimalAlgorithm.importObjects(rdd).computeUniquePrefixSum.collect()

      // then
    val expected = Array(-10, -17, -16, -15, -14, -12, -10, -5, 5, 17)
    assert(expected sameElements result.map(o => o._1))
    spark.stop()
  }
}
