package minimal_algorithms.spark.examples.ranking

import minimal_algorithms.spark.SparkMinAlgFactory
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

    new SparkMinAlgFactory(spark, numOfPartitions).rank(inputMapped, RankingObject.cmpKey).saveAsTextFile(outputPath)
    spark.stop()
  }
}
