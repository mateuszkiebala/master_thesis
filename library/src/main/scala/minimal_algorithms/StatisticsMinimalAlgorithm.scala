package minimal_algorithms

import minimal_algorithms.statistics_aggregators.StatisticsAggregator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
  * Class implementing base functions required to create a minimal algorithm with statistics computed on objects.
  * @param spark  SparkSession object
  * @param numOfPartitions  Number of partitions.
  */
class StatisticsMinimalAlgorithm[T <: Serializable]
  (spark: SparkSession, numOfPartitions: Int)(implicit ttag: ClassTag[T]) extends MinimalAlgorithm[T](spark, numOfPartitions) {

  /**
    * Applies prefix aggregation on imported objects. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @return RDD of pairs (prefixStatistics, object)
    */
  def prefix[K, S <: StatisticsAggregator[S]](cmpKey: T => K, statsAgg: T => S)
               (implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] = prefix(this.objects, cmpKey, statsAgg)

  /**
    * Computes prefix aggregation on provided RDD. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (prefixStatistics, object)
    */
  def prefix[K, S <: StatisticsAggregator[S]](rdd: RDD[T], cmpKey: T => K, statsAgg: T => S)
               (implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] = {
    val sortedRdd = teraSorted(rdd, cmpKey).persist()
    val prefixPartitionsStatistics = sendToAllHigherMachines(getPrefixedPartitionStatistics(sortedRdd, statsAgg).zip(List.range(1, this.numOfPartitions)))

    sortedRdd.zipPartitions(prefixPartitionsStatistics){(partitionIt, partitionPrefixIt) => {
      if (partitionIt.hasNext) {
        val elements = partitionIt.toList
        val prefixes = if (partitionPrefixIt.hasNext) {
          val partitionPrefix = partitionPrefixIt.next
          elements.map(e => statsAgg(e)).scanLeft(partitionPrefix)((res, a) => res.merge(a)).tail
        } else {
          elements.tail.scanLeft(statsAgg(elements.head)){(res, a) => res.merge(statsAgg(a))}
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
  def getPrefixedPartitionStatistics[S <: StatisticsAggregator[S]](rdd: RDD[T], statsAgg: T => S)(implicit stag: ClassTag[S]): Array[S] = {
    val elements = getPartitionsStatistics(rdd, statsAgg).collect()
    if (elements.isEmpty) {
      Array[S]()
    } else {
      elements.tail.scanLeft(elements.head){(res, a) => res.merge(a)}
    }
  }

  /**
    * Computes aggregated values for each partition.
    * @param rdd  Elements
    * @return RDD[aggregated value for partition]
    */
  def getPartitionsStatistics[S <: StatisticsAggregator[S]](rdd: RDD[T], statsAgg: T => S)(implicit stag: ClassTag[S]): RDD[S] = {
    rdd.mapPartitions(partition => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val elements = partition.toList
        Iterator(elements.tail.foldLeft(statsAgg(elements.head)){(acc, o) => acc.merge(statsAgg(o))})
      }
    })
  }
}
