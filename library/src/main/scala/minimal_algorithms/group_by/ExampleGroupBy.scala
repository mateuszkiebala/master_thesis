package minimal_algorithms.group_by

import minimal_algorithms.{ExampleMaoKey, MinimalAlgorithm}
import org.apache.spark.sql.SparkSession

object ExampleGroupBy {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleGroupBy").master("local").getOrCreate()

    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new ExampleMaoKey(p(0).toInt, p(1).toInt)})

    val minimalGroupBy = new MinimalGroupBy[ExampleMaoKey](spark, numOfPartitions).importObjects(inputMapped)
    val outputMA = new MinimalAlgorithm[ExampleMaoKey](spark, numOfPartitions).importObjects(minimalGroupBy.sum.map(p => new ExampleMaoKey(p._1, p._2.toInt)))
    outputMA.perfectSort.objects.map(res => res.getKey.toString + " " + res.getWeight.toString).saveAsTextFile(outputPath + "/output_sum")
    //minimalGroupBy.min.map(res => res._1.toString + " " + res._2.toInt.toString).saveAsTextFile(outputPath + "/min")
    //minimalGroupBy.max.map(res => res._1.toString + " " + res._2.toInt.toString).saveAsTextFile(outputPath + "/max")
    //minimalGroupBy.avg.map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(outputPath + "/avg")
    spark.stop()
  }
}
