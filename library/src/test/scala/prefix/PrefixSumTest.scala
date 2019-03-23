package prefix

//import minimal_algorithms.aggregations.{MaxAggregation, MinAggregation, SumAggregation}
import minimal_algorithms.{ExampleMao, MinimalAlgorithm}
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class PrefixTest extends FunSuite with SharedSparkContext with Matchers {
  val elements = Seq(new ExampleMao(1), new ExampleMao(5), new ExampleMao(-10), new ExampleMao(1), new ExampleMao(2),
    new ExampleMao(1), new ExampleMao(12), new ExampleMao(10), new ExampleMao(-7), new ExampleMao(2))
  val rdd = spark.sparkContext.parallelize(elements)

  /*test("Prefix sum") {
      // when
    val minimalAlgorithm = new MinimalAlgorithm[ExampleMao](spark, 2)
    val result = minimalAlgorithm.importObjects(rdd).computePrefix(new SumAggregation).collect()

      // then
    val expected = Array(-10, -17, -16, -15, -14, -12, -10, -5, 5, 17)
    assert(expected sameElements result.map(o => o._1))
  }

  test("Prefix min") {
      // when
    val minimalAlgorithm = new MinimalAlgorithm[ExampleMao](spark, 2)
    val result = minimalAlgorithm.importObjects(rdd).computePrefix(new MinAggregation).collect()

      // then
    val expected = Array(-10, -10, -10, -10, -10, -10, -10, -10, -10, -10)
    assert(expected sameElements result.map(o => o._1))
  }

  test("Prefix max") {
      // when
    val minimalAlgorithm = new MinimalAlgorithm[ExampleMao](spark, 2)
    val result = minimalAlgorithm.importObjects(rdd).computePrefix(new MaxAggregation).collect()

      // then
    val expected = Array(-10, -7, 1, 1, 1, 2, 2, 5, 10, 12)
    assert(expected sameElements result.map(o => o._1))
  }*/
}
