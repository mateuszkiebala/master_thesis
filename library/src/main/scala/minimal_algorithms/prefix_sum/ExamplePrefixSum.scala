package minimal_algorithms.prefix_sum

import minimal_algorithms.{MinimalAlgorithm, ExampleMao}
import org.apache.spark.sql.SparkSession

object ExamplePrefixSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExamplePrefixSum").master("local").getOrCreate()
    val inputPath = "test.txt"
    val outputPath = "out_prefix_sum"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new ExampleMao(p(1).toInt)})

    val minimalAlgorithm = new MinimalAlgorithm[ExampleMao](spark, 5)
    minimalAlgorithm.importObjects(inputMapped).computeUniquePrefixSum.saveAsTextFile(outputPath)
    spark.stop()
  }
}
