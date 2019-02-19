package minimal_algorithms.semi_join

import minimal_algorithms.MinimalSemiJoin
import minimal_algorithms.examples.{SemiJoinType, SemiJoinTypeEnum}
import org.apache.spark.sql.SparkSession

object ExampleSemiJoin {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleSemiJoin").master("local").getOrCreate()

    val inputPathR = "setR.txt"
    val inputPathT = "setT.txt"
    val outputPath = "out_semi_join"
    val inputR = spark.sparkContext.textFile(inputPathR)
    val inputMappedR = inputR.map(line => {
      val p = line.split(' ')
      new SemiJoinType(p(0).toInt, p(1).toInt, SemiJoinTypeEnum.RType)})

    val inputT = spark.sparkContext.textFile(inputPathT)
    val inputMappedT = inputT.map(line => {
      val p = line.split(' ')
      new SemiJoinType(p(0).toInt, p(1).toInt, SemiJoinTypeEnum.TType)})

    val minimalSemiJoin = new MinimalSemiJoin(spark, 5).importObjects(inputMappedR, inputMappedT)
    minimalSemiJoin.execute.collect.foreach(println)
    spark.stop()
  }
}
