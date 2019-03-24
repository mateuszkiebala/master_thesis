package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.sliding_aggregation.MinimalSlidingAggregation
import minimal_algorithms.statistics_aggregators.SumAggregator
import org.apache.spark.sql.SparkSession

object ExampleSlidingAggregation {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleSlidingAggregation").master("local").getOrCreate()

    //val inputPath = "hdfs://192.168.0.220:9000/user/test/input/input"
    //val numOfPartitions = args(0).toInt
    //val windowLen = args(1).toInt
    //val inputPath = args(2)
    //val outputPath = args(3)

    val numOfPartitions = 3
    val windowLen = 10
    val inputPath = "test.txt"

    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new SumSlidingSMAO(p(0).toInt, p(1).toDouble)})

    val msa = new MinimalSlidingAggregation[SumAggregator, SumSlidingSMAO](spark, numOfPartitions)
    //msa.sum(inputMapped, windowLen).map(res => res._1.toString + " " + res._2.toInt.toString).saveAsTextFile(outputPath)
    //msa.execute(inputMapped, windowLen).collect().foreach(println)
    //msa.max(inputMapped, windowLen).collect().foreach(println)
    //msa.avg(inputMapped, windowLen).collect().foreach(println)
    spark.stop()

  }
}
