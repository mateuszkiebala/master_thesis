package minimal_algorithms

import minimal_algorithms.statistics_aggregators.StatisticsAggregator
import minimal_algorithms.statistics_aggregators.StatisticsUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
  * Class implementing base functions required to create a minimal algorithm with statistics computed on objects.
  * @param spark  SparkSession object
  * @param numOfPartitions  Number of partitions.
  */
class StatisticsMinimalAlgorithm[T]
  (spark: SparkSession, numOfPartitions: Int)(implicit ttag: ClassTag[T]) extends MinimalAlgorithm[T](spark, numOfPartitions) {

  /**
    * Applies prefix aggregation on imported objects. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @return RDD of pairs (prefixStatistics, object)
    */
  def prefixed[K, S <: StatisticsAggregator[S]]
    (cmpKey: T => K, statsAgg: T => S)(implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] =
    prefix(this.objects, cmpKey, statsAgg)

  /**
    * Computes prefix aggregation on provided RDD. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (prefixStatistics, object)
    */
  def prefix[K, S <: StatisticsAggregator[S]]
    (rdd: RDD[T], cmpKey: T => K, statsAgg: T => S)(implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] = {
    val sortedRdd = teraSorted(rdd, cmpKey).persist()
    val distPartitionStatistics = sendToAllHigherMachines(partitionStatistics(sortedRdd, statsAgg).collect().zip(List.range(1, this.numOfPartitions)))

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

  /**
    * Computes prefix values on partitions' statistics
    * @param rdd  RDD of objects
    * @return Array of prefix statistics for partitions
    */
  def prefixedPartitionStatistics[S <: StatisticsAggregator[S]](rdd: RDD[T], statsAgg: T => S)(implicit stag: ClassTag[S]): Seq[S] = {
    val elements = partitionStatistics(rdd, statsAgg).collect()
    if (elements.isEmpty) Seq[S]() else scanLeft(elements)
  }

  /**
    * Computes aggregated values for each partition.
    * @param rdd  Elements
    * @return RDD[aggregated value for partition]
    */
  def partitionStatistics[S <: StatisticsAggregator[S]](rdd: RDD[T], statsAgg: T => S)(implicit stag: ClassTag[S]): RDD[S] = {
    rdd.mapPartitions(partition => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val start = partition.next
        Iterator(partition.foldLeft(statsAgg(start)){(acc, o) => acc.merge(statsAgg(o))})
      }
    })
  }
}
