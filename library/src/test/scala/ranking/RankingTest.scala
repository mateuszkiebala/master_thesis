package ranking

import minimal_algorithms.{MinimalAlgorithm, MinimalAlgorithmObject}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class RankObj(weight: Int, weightTwo: Int) extends MinimalAlgorithmObject[RankObj] {
  override def compareTo(o: RankObj): Int = {
    val res = this.weight.compareTo(o.getWeight)
    if (res != 0) res else this.weightTwo.compareTo(o.getWeightTwo)
  }

  override def getWeight: Int = {
    this.weight
  }

  def getWeightTwo: Int = {
    this.weightTwo
  }

  override def toString = {
    "W: " + this.weight + " | W2: " + this.weightTwo
  }
}

class RankingTest extends FunSuite {
  val spark = SparkSession.builder().appName("RankingTest").master("local").getOrCreate()
  val elements = Seq(new RankObj(2, 1), new RankObj(5, 0), new RankObj(-10, 0), new RankObj(1, 2), new RankObj(2, 2),
    new RankObj(1, 0), new RankObj(12, 1), new RankObj(10, 0), new RankObj(-7, 0), new RankObj(2, 0))
  val rdd = spark.sqlContext.sparkContext.parallelize(elements)

  test("Ranking") {
      // when
    val minimalRanking = new MinimalAlgorithm[RankObj](spark, 2).importObjects(rdd)

      // then
    val expected = Array((0, elements(2)), (1, elements(8)), (2, elements(5)), (3, elements(3)), (4, elements(9)),
      (5, elements(0)), (6, elements(4)), (7, elements(1)), (8, elements(7)), (9, elements(6)))
    val result = minimalRanking.computeUniqueRanking.collect()
    assert(expected.map(o => (o._1, o._2.getWeight, o._2.getWeightTwo)) sameElements result.map(o => (o._1, o._2.getWeight, o._2.getWeightTwo)))
    spark.stop()
  }
}
