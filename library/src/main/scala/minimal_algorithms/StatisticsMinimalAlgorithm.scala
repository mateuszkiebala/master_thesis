package minimal_algorithms

import minimal_algorithms.statistics_aggregators.StatisticsAggregator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
  * Class implementing base functions required to create a minimal algorithm with statistics computed on objects.
  * @param spark  SparkSession object
  * @param numOfPartitions  Number of partitions.
  * @tparam T T <: StatisticsMinimalAlgorithmObject[T] : ClassTag
  */
class StatisticsMinimalAlgorithm[T <: StatisticsMinimalAlgorithmObject[T] : ClassTag]
  (spark: SparkSession, numOfPartitions: Int) extends MinimalAlgorithm[T](spark, numOfPartitions) {

  /**
    * Applies prefix aggregation on imported objects. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @return RDD of pairs (prefixStatistics, object)
    */
  def computePrefix: RDD[(StatisticsAggregator, T)] = computePrefix(this.objects)

  /**
    * Computes prefix aggregation on provided RDD. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (prefixStatistics, object)
    */
  def computePrefix(rdd: RDD[T]): RDD[(StatisticsAggregator, T)] = {
    val sortedRdd = teraSorted(rdd).persist()
    val prefixPartitionsStatistics = sc.broadcast(getPrefixedPartitionStatistics(sortedRdd))
    sortedRdd.mapPartitionsWithIndex((index, partition) => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val smaoObjects = partition.toList
        val prefix = if (index == 0) {
          smaoObjects.tail.scanLeft(smaoObjects.head.getAggregator){(res, a) => res.merge(a.getAggregator)}
        } else {
          smaoObjects.map(smao => smao.getAggregator).scanLeft(prefixPartitionsStatistics.value(index-1))((res, a) => res.merge(a)).tail
        }
        smaoObjects.zipWithIndex.map{
          case (smao, i) => (prefix(i), smao)
          case _         => throw new Exception("Error while creating prefix values")
        }.toIterator
      }
    })
  }

  def prefix: RDD[(StatisticsAggregator, T)] = prefix(this.objects)

  def prefix(rdd: RDD[T]): RDD[(StatisticsAggregator, T)] = {
    val sortedRdd = teraSorted(rdd).persist()
    val prefixPartitionsStatistics = sendToAllHigherMachines(getPrefixedPartitionStatistics(sortedRdd).zip(List.range(1, this.numOfPartitions)))

    sortedRdd.zipPartitions(prefixPartitionsStatistics){(partitionIt, partitionPrefixIt) => {
      if (partitionIt.hasNext) {
        val elements = partitionIt.toList
        val prefixes = if (partitionPrefixIt.hasNext) {
          val partitionPrefix = partitionPrefixIt.next
          elements.map(e => e.getAggregator).scanLeft(partitionPrefix)((res, a) => res.merge(a)).tail
        } else {
          elements.tail.scanLeft(elements.head.getAggregator){(res, a) => res.merge(a.getAggregator)}
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
  def getPrefixedPartitionStatistics(rdd: RDD[T]): Array[StatisticsAggregator] = {
    val elements = getPartitionsStatistics(rdd).collect()
    if (elements.isEmpty) {
      Array()
    } else {
      elements.tail.scanLeft(elements.head){(res, a) => res.merge(a)}
    }
  }

  /**
    * Computes aggregated values for each partition.
    * @param rdd  Elements
    * @return RDD[aggregated value for partition]
    */
  def getPartitionsStatistics(rdd: RDD[T]): RDD[StatisticsAggregator] = {
    rdd.mapPartitions(partition => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val elements = partition.toList
        Iterator(elements.tail.foldLeft(elements.head.getAggregator){(acc, o) => acc.merge(o.getAggregator)})
      }
    })
  }
}
