package minimal_algorithms.ranking

import minimal_algorithms.{MinimalAlgorithm, ExampleMaoKey}
import org.apache.spark.sql.SparkSession

/**
  * Simulate ranking algorithm with use of prefix sums.
  */
object ExampleRankingUsingPrefixSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExampleRankingUsingPrefixSum").master("local").getOrCreate()
    val inputPath = "test.txt"
    val outputPath = "out_ranking_prefix_sum"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new ExampleMaoKey(p(1).toInt, 1)})

    val minimalAlgorithm = new MinimalAlgorithm[ExampleMaoKey](spark, 5)
    minimalAlgorithm.importObjects(inputMapped).computeUniquePrefixSum.collect().foreach(println)
    spark.stop()
  }
}