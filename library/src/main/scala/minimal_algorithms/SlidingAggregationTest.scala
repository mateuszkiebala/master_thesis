package minimal_algorithms

import org.apache.spark.sql.SparkSession

object SlidingAggregationTest {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("SlidingAggregationTest").master("local").getOrCreate()

    //val inputPath = "hdfs://192.168.0.220:9000/user/mati/test.txt"
    val windowLen = 4
    val inputPath = "test.txt"
    val outputPath = "out_sliding_agg"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new MyKW(p(0).toInt, p(1).toInt)})

    val msa = new MinimalSlidingAggregation[MyKW](spark, 5)
    msa.execute(inputMapped, windowLen).map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(outputPath)
    spark.stop()
  }
}
