package minimal_algorithms

import org.apache.spark.sql.SparkSession

object GroupByTest {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("GroupByTest").master("local").getOrCreate()

    val inputPath = "test.txt"
    val outputPath = "out_group_by_"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new MyKW(p(0).toInt, p(1).toInt)})

    val minimalGroupBy = new MinimalGroupBy[MyKW](spark, 5).importObjects(inputMapped).teraSort
    minimalGroupBy.groupBySum.saveAsTextFile(outputPath + "sum")
    minimalGroupBy.groupByMin.saveAsTextFile(outputPath + "min")
    spark.stop()
  }
}
