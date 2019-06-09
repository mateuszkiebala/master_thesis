import com.holdenkarau.spark.testing.SharedSparkContext
import minimal_algorithms.spark.SparkMinAlgFactory
import minimal_algorithms.spark.examples.ranking.RankingObject
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class RankingTest extends FunSuite with SharedSparkContext with Matchers {

  test("Ranking") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    var elements = Array((2, 1), (5, 0), (-10, 0), (1, 2), (2, 2), (1, 0), (12, 1), (10, 0), (-7, 0), (2, 0))
      .map{e => new RankingObject(e._1, e._2)}
    val rdd = spark.sparkContext.parallelize(elements)

    val result = new SparkMinAlgFactory(spark, 2, rdd).rank(RankingObject.cmpKey).collect()
    elements = elements.sortBy(RankingObject.cmpKey)
    assert(elements.sameElements(result.map{e => e._2}))
  }
}
