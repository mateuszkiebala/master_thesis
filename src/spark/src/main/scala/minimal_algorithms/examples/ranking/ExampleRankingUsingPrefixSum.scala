package minimal_algorithms.spark.examples.ranking

import minimal_algorithms.spark.MinimalAlgorithm
import minimal_algorithms.spark.examples.statistics_aggregators.SumAggregator
import org.apache.spark.sql.SparkSession

object ExampleRankingUsingPrefixSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExampleRankingUsingPrefixSum").master("local").getOrCreate()
    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new RankingObject(p(1).toInt, 1.0)})

    val cmpKey = (o: RankingObject) => new RankingComparator(o)
    val sumAgg = (o: RankingObject) => new SumAggregator(o.getWeight)
    new MinimalAlgorithm(spark, 5).prefix(inputMapped, cmpKey, sumAgg).collect().foreach(println)
    spark.stop()
  }
}
