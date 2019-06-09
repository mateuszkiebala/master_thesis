package minimal_algorithms.spark.examples.semi_join

import minimal_algorithms.spark.MinimalSemiJoin
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
      new SemiJoinType(p(0).toInt, p(1).toDouble, true)})

    val inputT = spark.sparkContext.textFile(inputPathT)
    val inputMappedT = inputT.map(line => {
      val p = line.split(' ')
      new SemiJoinType(p(0).toInt, p(1).toDouble, false)})

    val isRType = ((o: SemiJoinType) => o.getSetType)
    val minimalAlgorithm = new MinimalSemiJoin(spark, numOfPartitions)
    val result = minimalAlgorithm.semiJoin(inputMappedR, inputMappedT, SemiJoinType.cmpKey, isRType)
    minimalAlgorithm.perfectSort(result, SemiJoinType.cmpKey).map(res => res.toString).saveAsTextFile(outputPath)
    spark.stop()
  }
}
