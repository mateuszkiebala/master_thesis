package ranking


import minimal_algorithms.examples.ranking.RankingMAO
import minimal_algorithms.MinimalAlgorithm
import org.scalatest.{FunSuite, Matchers}
import setup.SharedSparkContext

class RankingTest extends FunSuite with SharedSparkContext with Matchers {
  test("Ranking") {
      // given
    val elements = Array((2, 1), (5, 0), (-10, 0), (1, 2), (2, 2), (1, 0), (12, 1), (10, 0), (-7, 0), (2, 0))
      .map{e => new RankingMAO(e._1, e._2)}
    val rdd = spark.sparkContext.parallelize(elements)

      // when
    val minimalRanking = new MinimalAlgorithm[RankingMAO](spark, 2).importObjects(rdd)

      // then
    assert(elements.sorted.sameElements(minimalRanking.computeRanking.collect().map{e => e._2}))
  }
}
