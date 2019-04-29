package ranking

import minimal_algorithms.examples.ranking.RankingObject
import minimal_algorithms.MinimalAlgorithm
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class RankingTest extends FunSuite with SharedSparkContext with Matchers {
  test("Ranking") {
      // given
    var elements = Array((2, 1), (5, 0), (-10, 0), (1, 2), (2, 2), (1, 0), (12, 1), (10, 0), (-7, 0), (2, 0))
      .map{e => new RankingObject(e._1, e._2)}
    val rdd = spark.sparkContext.parallelize(elements)

      // when
    val minimalRanking = new MinimalAlgorithm[RankingObject](spark, 2).importObjects(rdd)

      // then
    elements = elements.sortBy(RankingObject.cmpKey)
    assert(elements.sameElements(minimalRanking.ranked(RankingObject.cmpKey).collect().map{e => e._2}))
  }
}
