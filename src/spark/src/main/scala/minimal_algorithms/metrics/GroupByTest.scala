package minimal_algorithms.spark.metrics

import minimal_algorithms.spark.examples.statistics_aggregators.SumAggregator
import org.apache.spark.sql._
import minimal_algorithms.spark.SparkMinAlgFactory

object GroupByTest {

  def main(args: Array[String]): Unit = Utils.time {
    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val numOfItems = args(2).toInt

    val spark = SparkSession.builder().appName("GroupByTest").master("local").getOrCreate()
    val df = spark.read.format("com.databricks.spark.avro").option("header", "true").load(inputPath)
    val cmpKey = (o: Row) => o.getAs[Int]("int_prim")
    val sumAgg = (o: Row) => new SumAggregator(o.getAs[Int]("int_prim") % 10000)
    val result = new SparkMinAlgFactory(spark, numOfPartitions).groupBy(df.rdd, cmpKey, sumAgg, numOfItems)
    spark.stop()
  }
}
