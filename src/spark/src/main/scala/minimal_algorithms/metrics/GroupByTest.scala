package minimal_algorithms.spark.metrics

import minimal_algorithms.spark.examples.statistics_aggregators.SumAggregator
import org.apache.spark.sql._
import minimal_algorithms.spark.SparkMinAlgFactory

object GroupByTest {

  def main(args: Array[String]): Unit = Utils.time {
    val spark = SparkSession.builder().appName("GroupByTest").master("local").getOrCreate()
    val df = spark.read.format("com.databricks.spark.avro").option("header", "true").load("/user/mati/data.avro")
    val cmpKey = (o: Row) => o.getAs[Int]("int_prim")
    val sumAgg = (o: Row) => new SumAggregator(o.getAs[Int]("int_prim") % 10000)
    val result = new SparkMinAlgFactory(spark, 5).groupBy(df.rdd, cmpKey, sumAgg, 1000)
    result.collect().foreach(println)
    spark.stop()
  }
}
