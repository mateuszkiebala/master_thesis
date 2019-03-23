package minimal_algorithms.prefix

import minimal_algorithms.statistics_aggregators.SumAggregator
import minimal_algorithms.StatisticsMinimalAlgorithm
import org.apache.spark.sql.SparkSession

object ExamplePrefix {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExamplePrefix").master("local").getOrCreate()
    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)

    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new SumPrefixSMAO(p(1).toDouble)})

    val sma = new StatisticsMinimalAlgorithm[SumAggregator, SumPrefixSMAO](spark, numOfPartitions)
    sma.importObjects(inputMapped).computePrefix.saveAsTextFile(outputPath)
    spark.stop()
  }
}
