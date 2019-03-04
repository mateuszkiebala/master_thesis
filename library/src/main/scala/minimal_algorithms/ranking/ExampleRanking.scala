package minimal_algorithms.ranking

import minimal_algorithms.{MinimalAlgorithm, ExampleMao}
import org.apache.spark.sql.SparkSession

object ExampleRanking {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExampleRanking").master("local").getOrCreate()
    val inputPath = "test.txt"
    val outputPath = "out_ranking"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new ExampleMao(p(1).toInt)})

    val minimalAlgorithm = new MinimalAlgorithm[ExampleMao](spark, 5)
    minimalAlgorithm.importObjects(inputMapped).computeRanking.saveAsTextFile(outputPath)
    spark.stop()
  }
}
