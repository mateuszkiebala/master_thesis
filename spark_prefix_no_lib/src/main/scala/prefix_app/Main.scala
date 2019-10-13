// run: spark-submit --class PrefixApp --master yarn target/scala-2.11/prefixapp-project_2.11-1.0.jar [args]
// args: hdfs://192.168.0.199:9000/user/input <partitions_number> hdfs://192.168.0.199:9000/user/output

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PrefixApp {

  def prefix(sc: SparkContext, rdd: RDD[FourInts], numPartitions: Int): RDD[(Int, FourInts)] = {
    val sortedRdd = rdd.repartition(numPartitions).sortBy(FourInts.cmpKey).persist()

    val partStats = partitionStatistics(sortedRdd).collect()
    val prefixPartStats = partStats.scanLeft(0){(acc, o) => acc + o}
    val distPartStats = sc.broadcast(prefixPartStats).value

    sortedRdd.mapPartitionsWithIndex((pIndex, partitionIt) => {
      if (partitionIt.hasNext) {
        val elements = partitionIt.toList
        val prefixes = elements.scanLeft(distPartStats(pIndex)){(res, o) => res + o.getValue()}
        prefixes.zip(elements).iterator
      } else {
        Iterator()
      }
    })
  }

  def partitionStatistics(rdd: RDD[FourInts]): RDD[Int] = {
    rdd.mapPartitions(partitionIt => {
      if (partitionIt.isEmpty) {
        Iterator()
      } else {
        Iterator(partitionIt.foldLeft(0){(acc, o) => acc + o.getValue()})
      }
    })
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("PrefixApp").master("local").getOrCreate()
    import spark.implicits._

    val inputPath = args(0)
    val numOfPartitions = args(1).toInt
    val sc = spark.sparkContext
    val input = sc.textFile(inputPath)
    val inputMapped = input.map(line => {
            val p = line.split(' ')
            new FourInts(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toInt)})
    prefix(sc, inputMapped, numOfPartitions).map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(args(2))
    spark.stop()
  }
}
