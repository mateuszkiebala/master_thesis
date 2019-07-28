package minimal_algorithms.spark

import minimal_algorithms.spark.statistics.StatisticsAggregator
import minimal_algorithms.spark.statistics.StatisticsUtils.{partitionStatistics, foldLeft, scanLeft}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class MinimalAlgorithm(spark: SparkSession, numberOfPartitions: Int = -1) {
  protected val sc: SparkContext = spark.sparkContext
  protected var numPartitions: Int = numberOfPartitions;
  object PerfectPartitioner {}
  object KeyPartitioner {}

  def computeItemsNoByPartition[T](rdd: RDD[T], itemsCnt: Int = -1): (Int, Int) = {
    val cnt = if (itemsCnt < 0) rdd.count().toInt else itemsCnt
    ((cnt + numPartitions - 1) / numPartitions, cnt)
  }

  def teraSort[T, K](rdd: RDD[T], cmpKey: T => K)
                    (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[T] = {
    var newRdd = rdd;
    if (numberOfPartitions == -1) {
      numPartitions = rdd.getNumPartitions
    } else {
      newRdd = rdd.repartition(numPartitions)
    }
    newRdd.sortBy(cmpKey)
  }

  def rank[T, K](rdd: RDD[T], cmpKey: T => K)
                (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[(Int, T)] = {
    val sortedRdd = teraSort(rdd, cmpKey).persist()
    val distPartitionSizes = Utils.sendToAllHigherMachines(
      sc, Utils.partitionSizes(sortedRdd).collect().zip(List.range(0, numPartitions)), numPartitions
    )
    sortedRdd.zipPartitions(distPartitionSizes){(partitionIt, partitionSizesIt) => {
      if (partitionIt.hasNext) {
        val offset = partitionSizesIt.sum
        partitionIt.zipWithIndex.map{case (o, index) => (index + offset, o)}
      } else {
        Iterator()
      }
    }}
  }

  def perfectSort[T, K](rdd: RDD[T], cmpKey: T => K, itemsCnt: Int = -1)
                       (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[T] = {
    perfectSortWithRanks(rdd, cmpKey, itemsCnt).map(o => o._2)
  }

  def perfectSortWithRanks[T, K](rdd: RDD[T], cmpKey: T => K, itemsCnt: Int = -1)
                                (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[(Int, T)] = {
    rank(rdd, cmpKey).partitionBy(new PerfectPartitioner(numPartitions, computeItemsNoByPartition(rdd, itemsCnt)._1))
  }

  def prefix[T, K, S <: StatisticsAggregator[S]]
  (rdd: RDD[T], cmpKey: T => K, statsAgg: T => S)
  (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] = {
    val sortedRdd = teraSort(rdd, cmpKey).persist()
    val distPartitionStatistics = Utils.sendToAllHigherMachines(
      sc, partitionStatistics(sortedRdd, statsAgg).collect().zip(List.range(0, numPartitions)), numPartitions
    )
    sortedRdd.zipPartitions(distPartitionStatistics){(partitionIt, partitionStatisticsIt) => {
      if (partitionIt.hasNext) {
        val elements = partitionIt.toList
        val statistics = elements.map{e => statsAgg(e)}
        val prefixes = if (partitionStatisticsIt.hasNext) {
          scanLeft(statistics, foldLeft(partitionStatisticsIt)).drop(1)
        } else {
          scanLeft(statistics)
        }
        prefixes.zip(elements).iterator
      } else {
        Iterator()
      }
    }}
  }
}
