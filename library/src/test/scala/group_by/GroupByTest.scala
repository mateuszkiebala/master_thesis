package group_by

import minimal_algorithms.ExampleMaoKey
import minimal_algorithms.group_by.MinimalGroupBy
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class GroupByTest extends FunSuite with SharedSparkContext with Matchers {
  val elements = spark.sparkContext.parallelize(Seq(
    new ExampleMaoKey(1, 2), new ExampleMaoKey(1, 5), new ExampleMaoKey(1, -10),
    new ExampleMaoKey(2, 1), new ExampleMaoKey(10, 2), new ExampleMaoKey(5, 1), new ExampleMaoKey(10, 12),
    new ExampleMaoKey(2, 10), new ExampleMaoKey(10, -7), new ExampleMaoKey(5, 2), new ExampleMaoKey(10, 5)))

  test("GroupBy sum") {
      // when
    val minimalGroupBy = new MinimalGroupBy[ExampleMaoKey](spark, 2).importObjects(elements)

      // then
    assert(Set((1, -3.0), (2, 11.0), (5, 3.0), (10, 12.0)) == minimalGroupBy.sumGroupBy.collect().toSet)
  }

  test("GroupBy min") {
      // when
    val minimalGroupBy = new MinimalGroupBy[ExampleMaoKey](spark, 2).importObjects(elements)

      // then
    assert(Set((1, -10.0), (2, 1.0), (5, 1.0), (10, -7.0)) == minimalGroupBy.minGroupBy.collect().toSet)
  }

  test("GroupBy max") {
      // when
    val minimalGroupBy = new MinimalGroupBy[ExampleMaoKey](spark, 2).importObjects(elements)

      // then
    assert(Set((1, 5.0), (2, 10.0), (5, 2.0), (10, 12.0)) == minimalGroupBy.maxGroupBy.collect().toSet)
  }

  test("GroupBy average") {
      // when
    val minimalGroupBy = new MinimalGroupBy[ExampleMaoKey](spark, 2).importObjects(elements)

      // then
    assert(Set((1, -1.0), (2, 5.5), (5, 1.5), (10, 3.0)) == minimalGroupBy.averageGroupBy.collect().toSet)
  }
}
