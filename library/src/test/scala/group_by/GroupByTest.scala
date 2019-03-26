package group_by

import minimal_algorithms.examples.group_by.IntKey
import minimal_algorithms.group_by.{GroupByObject, MinimalGroupBy}
import minimal_algorithms.statistics_aggregators._
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class GroupByTest extends FunSuite with SharedSparkContext with Matchers {
  val elements = Array((1, 2), (1, 5), (1, -10), (2, 1), (10, 2), (5, 1), (10, 12), (2, 10), (10, -7), (5, 2), (10, 5))

  test("GroupBy sum") {
      // given
    val rdd = spark.sparkContext.parallelize(elements.map{e => new GroupByObject[IntKey](new SumAggregator(e._2), new IntKey(e._1))})

      // when
    val result = new MinimalGroupBy[GroupByObject[IntKey], IntKey](spark, 2).importObjects(rdd).execute

      // then
    assert(Set((1, -3.0), (2, 11.0), (5, 3.0), (10, 12.0)) == result.collect().map{case(k, v) => (k.getValue, v.asInstanceOf[SumAggregator].getValue)}.toSet)
  }

  test("GroupBy min") {
      // given
    val rdd = spark.sparkContext.parallelize(elements.map{e => new GroupByObject[IntKey](new MinAggregator(e._2), new IntKey(e._1))})

      // when
    val result = new MinimalGroupBy[GroupByObject[IntKey], IntKey](spark, 2).importObjects(rdd).execute

      // then
    assert(Set((1, -10.0), (2, 1.0), (5, 1.0), (10, -7.0)) == result.collect().map{case(k, v) => (k.getValue, v.asInstanceOf[MinAggregator].getValue)}.toSet)
  }

  test("GroupBy max") {
      // given
    val rdd = spark.sparkContext.parallelize(elements.map{e => new GroupByObject[IntKey](new MaxAggregator(e._2), new IntKey(e._1))})

      // when
    val result = new MinimalGroupBy[GroupByObject[IntKey], IntKey](spark, 2).importObjects(rdd).execute

      // then
    assert(Set((1, 5.0), (2, 10.0), (5, 2.0), (10, 12.0)) == result.collect().map{case(k, v) => (k.getValue, v.asInstanceOf[MaxAggregator].getValue)}.toSet)
  }

  test("GroupBy average") {
      // given
    val rdd = spark.sparkContext.parallelize(elements.map{e => new GroupByObject[IntKey](new AvgAggregator(e._2, 1), new IntKey(e._1))})

      // when
    val result = new MinimalGroupBy[GroupByObject[IntKey], IntKey](spark, 2).importObjects(rdd).execute

      // then
    assert(Set((1, -1.0), (2, 5.5), (5, 1.5), (10, 3.0)) == result.collect().map{case(k, v) => (k.getValue, v.asInstanceOf[AvgAggregator].getValue)}.toSet)
  }
}
