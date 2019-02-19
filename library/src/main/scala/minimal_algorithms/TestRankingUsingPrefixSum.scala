package minimal_algorithms

import org.apache.spark.sql.SparkSession

/**
  * Simulate ranking algorithm with use of prefix sums.
  */
object TestRankingUsingPrefixSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestRankingUsingPrefixSum").master("local").getOrCreate()
    val inputPath = "test.txt"
    val outputPath = "out_ranking_prefix_sum"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new TestMaoKey(p(1).toInt, 1)})

    val minimalAlgorithm = new MinimalAlgorithm[TestMaoKey](spark, 5)
    minimalAlgorithm.importObjects(inputMapped).computeUniquePrefixSum.collect().foreach(println)
    spark.stop()
  }
}
