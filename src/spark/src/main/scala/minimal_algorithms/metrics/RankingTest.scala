package minimal_algorithms.spark.metrics

import minimal_algorithms.spark.MinimalAlgorithm
import org.apache.spark.sql._

object RankingTest {

  def main(args: Array[String]): Unit = Utils.time {
    val spark = SparkSession.builder().appName("RankingSpeedTest").master("local").getOrCreate()
    val df = spark.read.format("com.databricks.spark.avro").option("header", "true").load("/user/mati/data.avro")
    val cmpKey = (o: Row) => o.getAs[Long]("long_prim")
    val result = new MinimalAlgorithm(spark, 5).rank(df.rdd, cmpKey)
    spark.stop()
  }
}
