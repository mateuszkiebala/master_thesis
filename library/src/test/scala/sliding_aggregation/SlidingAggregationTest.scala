package sliding_aggregation

import minimal_algorithms.examples.sliding_aggregation.{AvgSlidingSMAO, MaxSlidingSMAO, MinSlidingSMAO, SumSlidingSMAO}
import minimal_algorithms.sliding_aggregation._
import minimal_algorithms.statistics_aggregators.{AvgAggregator, MaxAggregator, MinAggregator, SumAggregator}
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class SlidingAggregationTest extends FunSuite with SharedSparkContext with Matchers {
  val elements = Array((1, 2), (3, 5), (2, -10), (4, 1), (7, 2), (6, 1), (5, 12), (8, 10), (10, -7), (9, 2), (11, 5))

  test("SlidingAggregation sum") {
      // given
    val rdd = spark.sparkContext.parallelize(elements.map{e => new SumSlidingSMAO(e._1, e._2)})

      // when
    val result = new MinimalSlidingAggregation[SumSlidingSMAO](spark, 3).execute(rdd, 7, SumSlidingSMAO.cmpKey, SumSlidingSMAO.statsAgg).collect()

      // then
    val expected = Array((1, 2.0), (2, -8.0), (3, -3.0), (4, -2.0), (5, 10.0), (6, 11.0), (7, 13.0), (8, 21.0), (9, 33.0), (10, 21.0), (11, 25.0))
    assert(expected.sameElements(result.map{e => (e._1.getKey, e._2.asInstanceOf[SumAggregator].getValue)}))
  }

  test("SlidingAggregation min") {
      // given
    val rdd = spark.sparkContext.parallelize(elements.map{e => new MinSlidingSMAO(e._1, e._2)})

      // when
    val result = new MinimalSlidingAggregation[MinSlidingSMAO](spark, 3).execute(rdd, 7, MinSlidingSMAO.cmpKey, MinSlidingSMAO.statsAgg).collect()

      // then
    val expected = Array((1, 2.0), (2, -10.0), (3, -10.0), (4, -10.0), (5, -10), (6, -10.0), (7, -10.0), (8, -10.0), (9, 1.0), (10, -7.0), (11, -7.0))
    assert(expected.sameElements(result.map{e => (e._1.getKey, e._2.asInstanceOf[MinAggregator].getValue)}))
  }

  test("SlidingAggregation max") {
      // given
    val rdd = spark.sparkContext.parallelize(elements.map{e => new MaxSlidingSMAO(e._1, e._2)})

      // when
    val result = new MinimalSlidingAggregation[MaxSlidingSMAO](spark, 3).execute(rdd, 7, MaxSlidingSMAO.cmpKey, MaxSlidingSMAO.statsAgg).collect()

      // then
    val expected = Array((1, 2.0), (2, 2.0), (3, 5.0), (4, 5.0), (5, 12.0), (6, 12.0), (7, 12.0), (8, 12.0), (9, 12.0), (10, 12.0), (11, 12.0))
    assert(expected.sameElements(result.map{e => (e._1.getKey, e._2.asInstanceOf[MaxAggregator].getValue)}))
  }


  test("SlidingAggregation average") {
      // given
    val rdd = spark.sparkContext.parallelize(elements.map{e => new AvgSlidingSMAO(e._1, e._2)})

      // when
    val result = new MinimalSlidingAggregation[AvgSlidingSMAO](spark, 3).execute(rdd, 7, AvgSlidingSMAO.cmpKey, AvgSlidingSMAO.statsAgg).collect()

      // then
    val expected = Array((1, 2.0), (2, -4.0), (3, -1.0), (4, -0.5), (5, 2.0), (6, 11.0 / 6), (7, 13.0 / 7), (8, 3.0), (9, 33.0 / 7), (10, 3.0), (11, 25.0 / 7))
    assert(expected.sameElements(result.map{e => (e._1.getKey, e._2.asInstanceOf[AvgAggregator].getValue)}))
  }
}
