package minimal_algorithms.sliding_aggregation

import minimal_algorithms.ExampleMaoKey
import org.apache.spark.sql.SparkSession

object ExampleSlidingAggregation {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleSlidingAggregation").master("local").getOrCreate()

    //val inputPath = "hdfs://192.168.0.220:9000/user/mati/test.txt"
    val windowLen = 4
    val inputPath = "test.txt"
    val outputPath = "out_sliding_agg"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new ExampleMaoKey(p(0).toInt, p(1).toInt)})

    val msa = new MinimalSlidingAggregation[ExampleMaoKey](spark, 5)
    msa.sum(inputMapped, windowLen).collect().foreach(println)//.map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(outputPath)
    msa.min(inputMapped, windowLen).collect().foreach(println)
    msa.max(inputMapped, windowLen).collect().foreach(println)
    msa.avg(inputMapped, windowLen).collect().foreach(println)
    spark.stop()
  }
}
