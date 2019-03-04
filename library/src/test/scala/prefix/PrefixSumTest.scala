package prefix_sum

import minimal_algorithms.aggregation_function.SumAggregation
import minimal_algorithms.{ExampleMao, MinimalAlgorithm}
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class PrefixSumTest extends FunSuite with SharedSparkContext with Matchers {
  val elements = Seq(new ExampleMao(1), new ExampleMao(5), new ExampleMao(-10), new ExampleMao(1), new ExampleMao(2),
    new ExampleMao(1), new ExampleMao(12), new ExampleMao(10), new ExampleMao(-7), new ExampleMao(2))
  val rdd = spark.sparkContext.parallelize(elements)

  test("Prefix sum") {
      // when
    val minimalAlgorithm = new MinimalAlgorithm[ExampleMao](spark, 2)
    val result = minimalAlgorithm.importObjects(rdd).computePrefix(new SumAggregation).collect()

      // then
    val expected = Array(-10, -17, -16, -15, -14, -12, -10, -5, 5, 17)
    assert(expected sameElements result.map(o => o._1))
  }
}
