package minimal_algorithms

import org.apache.spark.sql.SparkSession

object TestRanking {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestRanking").master("local").getOrCreate()
    val inputPath = "test.txt"
    val outputPath = "out_ranking"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new TestMao(p(1).toInt)})

    val minimalAlgorithm = new MinimalAlgorithm[TestMao](spark, 5)
    minimalAlgorithm.importObjects(inputMapped).teraSort.computeRanking.saveAsTextFile(outputPath)
    spark.stop()
  }
}
