package minimal_algorithms.examples.ranking

import minimal_algorithms.MinimalAlgorithm
import org.apache.spark.sql.SparkSession

object ExampleRanking {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExampleRanking").master("local").getOrCreate()
    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new RankingMAO(p(0).toInt, p(1).toDouble)})

    val minimalAlgorithm = new MinimalAlgorithm[RankingMAO](spark, numOfPartitions)
    minimalAlgorithm.importObjects(inputMapped).computeRanking.saveAsTextFile(outputPath)
    spark.stop()
  }
}
