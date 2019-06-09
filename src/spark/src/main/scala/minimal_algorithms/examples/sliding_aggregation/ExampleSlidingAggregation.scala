package minimal_algorithms.spark.examples.sliding_aggregation

import minimal_algorithms.spark.examples.statistics_aggregators.SumAggregator
import minimal_algorithms.spark.sliding_aggregation.MinimalSlidingAggregation
import org.apache.spark.sql.SparkSession

class InputObject(key: Int, weight: Int) extends Serializable {
  def getWeight: Double = weight
  def getKey: Int = key
  override def toString: String = this.key.toString
}

object ExampleSlidingAggregation {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleSlidingAggregation").master("local").getOrCreate()

    val numOfPartitions = args(0).toInt
    val windowLen = args(1).toInt
    val inputPath = args(2)
    val outputPath = args(3)

    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new InputObject(p(0).toInt, p(1).toInt)})

    val cmpKey = (o: InputObject) => o.getKey
    val minimalAlgorithm = new MinimalSlidingAggregation(spark, numOfPartitions)

    val sumAgg = (o: InputObject) => new SumAggregator(o.getWeight)
    minimalAlgorithm.aggregate(inputMapped, windowLen, cmpKey, sumAgg).
      map(res => res._1.toString + " " + res._2.getValue.toInt.toString).saveAsTextFile(outputPath)
    spark.stop()
  }
}
