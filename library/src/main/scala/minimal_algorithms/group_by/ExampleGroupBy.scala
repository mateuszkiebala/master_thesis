package minimal_algorithms.group_by

import minimal_algorithms.ExampleMaoKey
import org.apache.spark.sql.SparkSession

object ExampleGroupBy {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleGroupBy").master("local").getOrCreate()

    val inputPath = "test.txt"
    val outputPath = "out_group_by_"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new ExampleMaoKey(p(0).toInt, p(1).toInt)})

    val minimalGroupBy = new MinimalGroupBy[ExampleMaoKey](spark, 5).importObjects(inputMapped)
    minimalGroupBy.groupBySum.saveAsTextFile(outputPath + "sum")
    minimalGroupBy.groupByMin.saveAsTextFile(outputPath + "min")
    spark.stop()
  }
}
