import com.holdenkarau.spark.testing.SharedSparkContext
import minimal_algorithms.MinimalSemiJoin
import minimal_algorithms.examples.semi_join.SemiJoinType
import minimal_algorithms.semi_join.SemiJoinSetTypeEnum
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class SemiJoinTest extends FunSuite with SharedSparkContext with Matchers {

  test("SemiJoin") {
      // given
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    val setR = Array((1, 2), (1, -4), (2, 10), (-5, 1), (5, 2), (1, 5), (-5099, 1), (-500, 2), (-100, 5))
    val setT = Array((1, 4), (-5, 10), (1, 11), (100, 12), (100, 34), (900, 23))
    val rddR = spark.sparkContext.parallelize(setR.map{e => new SemiJoinType(e._1, e._2, SemiJoinSetTypeEnum.RType)})
    val rddT = spark.sparkContext.parallelize(setT.map{e => new SemiJoinType(e._1, e._2, SemiJoinSetTypeEnum.TType)})

      // when
    val result = new MinimalSemiJoin(spark, 2).semiJoin(rddR, rddT, SemiJoinType.cmpKey).collect

      // then
    val expected = Set(setR(0), setR(1), setR(3), setR(5))
    assert(expected.map(o => (o._1, o._2, SemiJoinSetTypeEnum.RType)) == result.map(o => (o.getKey, o.getWeight, o.getSetType)).toSet)
  }
}
