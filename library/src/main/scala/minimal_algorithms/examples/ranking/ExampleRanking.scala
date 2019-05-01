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
      new RankingObject(p(0).toInt, p(1).toDouble)})

    val minimalAlgorithm = new MinimalAlgorithm(spark, numOfPartitions)
    minimalAlgorithm.rank(inputMapped, RankingObject.cmpKey).saveAsTextFile(outputPath)
    spark.stop()
  }
}
