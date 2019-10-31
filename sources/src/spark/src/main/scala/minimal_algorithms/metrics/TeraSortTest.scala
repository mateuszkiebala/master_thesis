package minimal_algorithms.spark.metrics

import minimal_algorithms.spark.MinimalAlgorithm
import org.apache.spark.sql._
import minimal_algorithms.spark.SparkMinAlgFactory

object TeraSortTest {

  def main(args: Array[String]): Unit = Utils.time {
    val numOfPartitions = args(0).toInt
    val inputPath = args(1)

    val spark = SparkSession.builder().appName("TeraSortTest").getOrCreate()
    val df = spark.read.format("com.databricks.spark.avro").option("header", "true").load(inputPath)
    val cmpKey = (o: Row) => o.getAs[Long]("long_prim")
    val result = new SparkMinAlgFactory(spark, numOfPartitions).teraSort(df.rdd, cmpKey)
    spark.stop()
  }
}
