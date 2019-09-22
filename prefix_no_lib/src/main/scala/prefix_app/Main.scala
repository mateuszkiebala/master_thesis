// run: spark-submit --class PrefixApp --master yarn target/scala-2.11/prefixapp-project_2.11-1.0.jar [args]
// args: hdfs://192.168.0.199:9000/user/input <window> hdfs://192.168.0.199:9000/user/output <partitions>

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

case class KeyPartitioner(numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }
}

object PrefixApp {

  object KeyPartitioner {}

  def prefix(sc: SparkContext, rdd: RDD[FourInts], numPartitions: Int): RDD[(Int, FourInts)] = {
    val sortedRdd = rdd.repartition(numPartitions).sortBy(FourInts.cmpKey).persist()

   val distPartitionStatistics = sendStatisticsToAllHigherMachines(
     sc.parallelize(partitionStatistics(sortedRdd).collect().zip(List.range(0, numPartitions)), numPartitions)
   )

    sortedRdd.zipPartitions(distPartitionStatistics){(partitionIt, partitionStatisticsIt) => {
      if (partitionIt.hasNext) {
        val elements = partitionIt.toList
        val prefixes = elements.scanLeft(partitionStatisticsIt.sum){(res, o) => res + o.getValue()}
        prefixes.zip(elements).iterator
      } else {
        Iterator()
      }
    }}
  }

  def sendStatisticsToAllHigherMachines(rdd: RDD[(Int, Int)]): RDD[Int] = {
    val numPartitions = rdd.getNumPartitions
    partitionByKey(rdd.mapPartitions(partition => {
      partition.flatMap{case (o, lowerBound) => List.range(lowerBound + 1, numPartitions).map{i => (i, o)}}
    }))
  }

  def partitionByKey(rdd: RDD[(Int, Int)]): RDD[Int] = {
    val numPartitions = rdd.getNumPartitions
    rdd.partitionBy(new KeyPartitioner(numPartitions)).map(x => x._2)
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
