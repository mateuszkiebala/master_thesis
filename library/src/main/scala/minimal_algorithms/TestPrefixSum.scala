package minimal_algorithms

import org.apache.spark.sql.SparkSession

object TestPrefixSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestPrefixSum").master("local").getOrCreate()
    val inputPath = "test.txt"
    val outputPath = "out_prefix_sum"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new TestMao(p(1).toInt)})

    val minimalAlgorithm = new MinimalAlgorithm[TestMao](spark, 5)
    minimalAlgorithm.importObjects(inputMapped).teraSort.computePrefixSum.saveAsTextFile(outputPath)
    spark.stop()
  }
}
