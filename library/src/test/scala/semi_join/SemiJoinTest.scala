package semi_join

import minimal_algorithms.MinimalSemiJoin
import minimal_algorithms.examples.{SemiJoinType}
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class SemiJoinTest extends FunSuite {
  val spark = SparkSession.builder().appName("SemiJoinTest").master("local").getOrCreate()
  val setR = Seq(new SemiJoinType(1, 2, 0), new SemiJoinType(1, -4, 0), new SemiJoinType(2, 10, 0),
    new SemiJoinType(-5, 1, 0), new SemiJoinType(5, 2, 0), new SemiJoinType(1, 5, 0))
  val setT = Seq(new SemiJoinType(1, 4, 1), new SemiJoinType(-5, 10, 1), new SemiJoinType(1, 11, 1))
  val rddR = spark.sqlContext.sparkContext.parallelize(setR)
  val rddT = spark.sqlContext.sparkContext.parallelize(setT)

  test("SemiJoin") {
      // when
    val minimalSemiJoin = new MinimalSemiJoin(spark, 2).importObjects(rddR, rddT)
    val result = minimalSemiJoin.execute.collect

      // then
    val expected = Set(setR(0), setR(1), setR(3), setR(5))
    assert(expected.map(o => (o.getKey, o.getWeight, o.getSetType)) == result.map(o => (o.getKey, o.getWeight, o.getSetType)).toSet)
    spark.stop()
  }
}
