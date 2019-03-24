package minimal_algorithms.examples.semi_join

import minimal_algorithms.{MinimalAlgorithm, MinimalSemiJoin}
import minimal_algorithms.semi_join.SemiJoinSetTypeEnum
import org.apache.spark.sql.SparkSession

object ExampleSemiJoin {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleSemiJoin").master("local").getOrCreate()

    val numOfPartitions = args(0).toInt
    val inputPathR = args(1)
    val inputPathT = args(2)
    val outputPath = args(3)
    val inputR = spark.sparkContext.textFile(inputPathR)
    val inputMappedR = inputR.map(line => {
      val p = line.split(' ')
      new SemiJoinType(p(0).toInt, p(1).toDouble, SemiJoinSetTypeEnum.RType)})

    val inputT = spark.sparkContext.textFile(inputPathT)
    val inputMappedT = inputT.map(line => {
      val p = line.split(' ')
      new SemiJoinType(p(0).toInt, p(1).toDouble, SemiJoinSetTypeEnum.TType)})

    val minimalSemiJoin = new MinimalSemiJoin(spark, numOfPartitions).importObjects(inputMappedR, inputMappedT)
    val outputMA = new MinimalAlgorithm[SemiJoinType](spark, numOfPartitions).importObjects(minimalSemiJoin.execute)
    //outputMA.perfectSort.objects.map(res => res.getKey.toString + " " + res.getWeight.toInt.toString).saveAsTextFile(outputPath)
    spark.stop()
  }
}
