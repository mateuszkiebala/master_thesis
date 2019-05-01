package minimal_algorithms.examples.group_by

import minimal_algorithms.examples.statistics_aggregators.{AvgAggregator, MaxAggregator, MinAggregator, SumAggregator}
import minimal_algorithms.group_by.MinimalGroupBy
import org.apache.spark.sql.SparkSession

class InputObject(key: Int, weight: Double) extends Serializable {
  def getKey: Int = this.key
  def getWeight: Double = this.weight
  override def toString: String = key + " " + weight
}

object ExampleGroupBy {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleGroupBy").master("local").getOrCreate()

    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)
    val input = spark.sparkContext.textFile(inputPath)

    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new InputObject(p(0).toInt, p(1).toDouble)
    })
    val cmpKey = (o: InputObject) => o.getKey
    val resultCmpKey = (o: (Int, _)) => o._1
    val minimalAlgorithm = new MinimalGroupBy(spark, numOfPartitions)

    val sumAgg = (o: InputObject) => new SumAggregator(o.getWeight)
    val groupedSum = minimalAlgorithm.groupBy(inputMapped, cmpKey, sumAgg)
    minimalAlgorithm.perfectSort(groupedSum, resultCmpKey).map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(outputPath + "/output_sum")

    val minAgg = (o: InputObject) => new MinAggregator(o.getWeight)
    val groupedMin = minimalAlgorithm.groupBy(inputMapped, cmpKey, minAgg)
    minimalAlgorithm.perfectSort(groupedMin, resultCmpKey).map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(outputPath + "/output_min")

    val maxAgg = (o: InputObject) => new MaxAggregator(o.getWeight)
    val groupedMax = minimalAlgorithm.groupBy(inputMapped, cmpKey, maxAgg)
    minimalAlgorithm.perfectSort(groupedMax, resultCmpKey).map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(outputPath + "/output_max")

    val avgAgg = (o: InputObject) => new AvgAggregator(o.getWeight, 1)
    val groupedAvg = minimalAlgorithm.groupBy(inputMapped, cmpKey, avgAgg)
    minimalAlgorithm.perfectSort(groupedAvg, resultCmpKey).map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(outputPath + "/output_avg")

    spark.stop()
  }
}
