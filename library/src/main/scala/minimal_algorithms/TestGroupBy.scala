package minimal_algorithms

import org.apache.spark.sql.SparkSession

object TestGroupBy {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("TestGroupBy").master("local").getOrCreate()

    val inputPath = "test.txt"
    val outputPath = "out_group_by_"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new TestMaoKey(p(0).toInt, p(1).toInt)})

    val minimalGroupBy = new MinimalGroupBy[TestMaoKey](spark, 5).importObjects(inputMapped).teraSort
    minimalGroupBy.groupBySum.saveAsTextFile(outputPath + "sum")
    minimalGroupBy.groupByMin.saveAsTextFile(outputPath + "min")
    spark.stop()
  }
}
