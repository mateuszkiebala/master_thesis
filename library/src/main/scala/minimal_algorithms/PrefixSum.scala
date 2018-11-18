package minimal_algorithms

import org.apache.spark.sql.SparkSession

object PrefixSum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Ranking").master("local").getOrCreate()
    val inputPath = "/Users/mateuszkiebala/Documents/studia/magisterka/library/test.txt"
    val outputPath = "/Users/mateuszkiebala/Documents/studia/magisterka/library/out_prefix_sum"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new MyW(p(1).toInt)})

    val minimalAlgorithm = new MinimalAlgorithm[MyW](spark, 5)
    minimalAlgorithm.importObjects(inputMapped).teraSort.computePrefixSum.saveAsTextFile(outputPath)
    spark.stop()
  }
}
