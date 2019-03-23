package sliding_aggregation

import minimal_algorithms.ExampleMaoKey
import minimal_algorithms.sliding_aggregation.MinimalSlidingAggregation
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class SlidingAggregationTest extends FunSuite with SharedSparkContext with Matchers {
  val elements = spark.sparkContext.parallelize(Seq(
    new ExampleMaoKey(1, 2), new ExampleMaoKey(3, 5), new ExampleMaoKey(2, -10),
    new ExampleMaoKey(4, 1), new ExampleMaoKey(7, 2), new ExampleMaoKey(6, 1), new ExampleMaoKey(5, 12),
    new ExampleMaoKey(8, 10), new ExampleMaoKey(10, -7), new ExampleMaoKey(9, 2), new ExampleMaoKey(11, 5)))
  val minimalSlidingAggregation = new MinimalSlidingAggregation[ExampleMaoKey](spark, 3).importObjects(elements)

/*  test("SlidingAggregation sum") {
      // when
    val result = minimalSlidingAggregation.sum(elements, 7).collect()

      // then
    val expected = Array((1, 2.0), (2, -8.0), (3, -3.0), (4, -2.0), (5, 10.0), (6, 11.0), (7, 13.0), (8, 21.0), (9, 33.0), (10, 21.0), (11, 25.0))
    assert(expected sameElements result)
  }

  test("SlidingAggregation min") {
      // when
    val result = minimalSlidingAggregation.min(elements, 7).collect()

      // then
    val expected = Array((1, 2.0), (2, -10.0), (3, -10.0), (4, -10.0), (5, -10), (6, -10.0), (7, -10.0), (8, -10.0), (9, 1.0), (10, -7.0), (11, -7.0))
    assert(expected sameElements result)
  }

  test("SlidingAggregation max") {
      // when
    val result = minimalSlidingAggregation.max(elements, 7).collect()

      // then
    val expected = Array((1, 2.0), (2, 2.0), (3, 5.0), (4, 5.0), (5, 12.0), (6, 12.0), (7, 12.0), (8, 12.0), (9, 12.0), (10, 12.0), (11, 12.0))
    assert(expected sameElements result)
  }

  test("SlidingAggregation average") {
      // when
    val result = minimalSlidingAggregation.avg(elements, 7).collect()

      // then
    val expected = Array((1, 2.0), (2, -4.0), (3, -1.0), (4, -0.5), (5, 2.0), (6, 11.0 / 6), (7, 13.0 / 7), (8, 3.0), (9, 33.0 / 7), (10, 3.0), (11, 25.0 / 7))
    assert(expected sameElements result)
  }*/
}
