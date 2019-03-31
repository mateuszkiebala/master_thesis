package minimal_algorithms.examples.ranking

import minimal_algorithms.StatisticsMinimalAlgorithm
import minimal_algorithms.examples.prefix.PrefixSMAO
import minimal_algorithms.statistics_aggregators.SumAggregator
import org.apache.spark.sql.SparkSession

/**
  * Simulate ranking algorithm with use of prefix sums.
  */

class RankAsPrefixSMAO(key: Int, weight: Double) extends PrefixSMAO[RankAsPrefixSMAO](weight) {
  override def compareTo(that: RankAsPrefixSMAO): Int = {
    this.key.compareTo(that.getKey)
  }

  override def getAggregator: SumAggregator = {
    new SumAggregator(this.weight)
  }

  def getKey: Int = this.key
}

object ExampleRankingUsingPrefixSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExampleRankingUsingPrefixSum").master("local").getOrCreate()
    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new RankAsPrefixSMAO(p(1).toInt, 1.0)})

    val minimalAlgorithm = new StatisticsMinimalAlgorithm[RankAsPrefixSMAO](spark, 5)
    minimalAlgorithm.importObjects(inputMapped).computePrefix.collect().foreach(println)
    spark.stop()
  }
}
