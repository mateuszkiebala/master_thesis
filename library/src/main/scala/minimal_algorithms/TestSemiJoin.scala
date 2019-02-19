package minimal_algorithms

import org.apache.spark.sql.SparkSession

object TestSemiJoin {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("TestSemiJoin").master("local").getOrCreate()

    val inputPathR = "setR.txt"
    val inputPathT = "setT.txt"
    val outputPath = "out_semi_join"
    val inputR = spark.sparkContext.textFile(inputPathR)
    val inputMappedR = inputR.map(line => {
      val p = line.split(' ')
      new TestSemiJoinType(p(0).toInt, p(1).toInt, MySemiJoinType.RType)})

    val inputT = spark.sparkContext.textFile(inputPathT)
    val inputMappedT = inputT.map(line => {
      val p = line.split(' ')
      new TestSemiJoinType(p(0).toInt, p(1).toInt, MySemiJoinType.TType)})

    val minimalGroupBy = new MinimalSemiJoin(spark, 5).importObjects(inputMappedR, inputMappedT).teraSort
    minimalGroupBy.execute.collect.foreach(println)
    spark.stop()
  }
}
