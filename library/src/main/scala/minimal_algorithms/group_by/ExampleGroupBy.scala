package minimal_algorithms.group_by

import minimal_algorithms.MinimalAlgorithm
import org.apache.spark.sql.SparkSession
/*
object ExampleGroupBy {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleGroupBy").master("local").getOrCreate()

    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new ExampleMaoKey(p(0).toInt, p(1).toDouble)})

    val minimalGroupBy = new MinimalGroupBy[ExampleMaoKey](spark, numOfPartitions).importObjects(inputMapped)
    var outputMA = new MinimalAlgorithm[ExampleMaoKey](spark, numOfPartitions).importObjects(minimalGroupBy.sum.map(p => new ExampleMaoKey(p._1, p._2.toInt)))
    outputMA.perfectSort.objects.map(res => res.getKey.toString + " " + res.getWeight.toInt.toString).saveAsTextFile(outputPath + "/output_sum")

    outputMA = new MinimalAlgorithm[ExampleMaoKey](spark, numOfPartitions).importObjects(minimalGroupBy.min.map(p => new ExampleMaoKey(p._1, p._2.toInt)))
    outputMA.perfectSort.objects.map(res => res.getKey.toString + " " + res.getWeight.toInt.toString).saveAsTextFile(outputPath + "/output_min")

    outputMA = new MinimalAlgorithm[ExampleMaoKey](spark, numOfPartitions).importObjects(minimalGroupBy.max.map(p => new ExampleMaoKey(p._1, p._2.toInt)))
    outputMA.perfectSort.objects.map(res => res.getKey.toString + " " + res.getWeight.toInt.toString).saveAsTextFile(outputPath + "/output_max")

    outputMA = new MinimalAlgorithm[ExampleMaoKey](spark, numOfPartitions).importObjects(minimalGroupBy.avg.map(p => new ExampleMaoKey(p._1, p._2)))
    outputMA.perfectSort.objects.map(res => res.getKey.toString + " " + "%.6f".format(res.getWeight)).saveAsTextFile(outputPath + "/output_avg")

    spark.stop()
  }
}
*/
