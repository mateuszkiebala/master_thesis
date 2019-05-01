package minimal_algorithms

import minimal_algorithms.statistics_aggregators.StatisticsAggregator
import minimal_algorithms.statistics_aggregators.StatisticsUtils.{foldLeft, scanLeft}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Class implementing base functions required to create a minimal algorithm.
  * @param spark  SparkSession object
  * @param numPartitions  Number of partitions.
  */
class MinimalAlgorithm[T](spark: SparkSession, numPartitions: Int)(implicit ttag: ClassTag[T]) {
  protected val sc = spark.sparkContext
  var objects: RDD[T] = sc.emptyRDD
  var itemsTotalCnt: Int = 0
  var itemsCntByPartition: Int = 0
  object PerfectPartitioner {}
  object KeyPartitioner {}

  /**
    * Imports objects.
    * @param rdd  Objects to be processed by minimal algorithm.
    * @return this
    */
  def importObjects(rdd: RDD[T], itemsTotalCnt: Int = -1): this.type = {
    objects = rdd.repartition(numPartitions)
    this.itemsTotalCnt = if (itemsTotalCnt < 0) objects.count().toInt else itemsTotalCnt
    itemsCntByPartition = computeItemsCntByPartition(objects, this.itemsTotalCnt)
    this
  }

  def computeItemsCntByPartition(rdd: RDD[T], itemsTotalCnt: Int = -1): Int = {
    val cnt = if (itemsTotalCnt < 0) rdd.count().toInt else itemsTotalCnt
    (cnt + numPartitions - 1) / numPartitions
  }

  /**
    * Applies Tera Sort algorithm on imported objects. Function affects imported objects.
    * @return this
    */
  def teraSort[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): this.type = {
    objects = objects.sortBy(cmpKey).persist()
    this
  }

  /**
    * Applies Tera Sort algorithm on provided RDD.
    * @param rdd  RDD on which Tera Sort will be performed.
    * @return Sorted RDD.
    */
  def teraSorted[K](rdd: RDD[T], cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    rdd.repartition(numPartitions).sortBy(cmpKey)
  }

  /**
    * Applies ranking algorithm on imported objects. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @return RDD of pairs (ranking, object)
    */
  def rank[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] =
    ranked(objects, cmpKey)

  /**
    * Applies ranking algorithm on given RDD. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (ranking, object)
    */
  def ranked[K](rdd: RDD[T], cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    val sortedRdd = teraSorted(rdd, cmpKey).persist()
    val distPartitionSizes = Utils.sendToAllHigherMachines(sc, Utils.partitionSizes(sortedRdd).collect().zip(List.range(1, numPartitions)), numPartitions)
    sortedRdd.zipPartitions(distPartitionSizes){(partitionIt, partitionSizesIt) => {
      if (partitionIt.hasNext) {
        val offset = partitionSizesIt.sum
        partitionIt.zipWithIndex.map{case (o, index) => (index + offset, o)}
      } else {
        Iterator()
      }
    }}
  }

  /**
    * Sorts and perfectly balances imported objects. Function affects imported objects.
    * @return this
    */
  def perfectSort[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    objects = perfectlySortedWithRanks(objects, cmpKey, itemsTotalCnt).map(o => o._2).persist()
    objects
  }

  /**
    * Sorts and perfectly balances provided RDD.
    * @param rdd  RDD with objects to process.
    * @return Perfectly balanced RDD of objects
    */
  def perfectlySorted[K](rdd: RDD[T], cmpKey: T => K, itemsTotalCnt: Int = -1)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    perfectlySortedWithRanks(rdd, cmpKey, itemsTotalCnt).map(o => o._2)
  }

  /**
    * Sorts and perfectly balances imported objects.
    * @return Perfectly balanced RDD of pairs (ranking, object)
    */
  def perfectSortWithRanks[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    perfectlySortedWithRanks(objects, cmpKey, itemsTotalCnt)
  }

  /**
    * Sorts and perfectly balances provided RDD.
    * @param rdd  RDD with objects to process.
    * @return Perfectly balanced RDD of pairs (ranking, object)
    */
  def perfectlySortedWithRanks[K](rdd: RDD[T], cmpKey: T => K, itemsTotalCnt: Int = -1)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    ranked(rdd, cmpKey).partitionBy(new PerfectPartitioner(numPartitions, computeItemsCntByPartition(rdd, itemsTotalCnt)))
  }

  /**
    * Applies prefix aggregation on imported objects. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @return RDD of pairs (prefixStatistics, object)
    */
  def prefixed[K, S <: StatisticsAggregator[S]]
  (cmpKey: T => K, statsAgg: T => S)(implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] =
    prefix(objects, cmpKey, statsAgg)

  /**
    * Computes prefix aggregation on provided RDD. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (prefixStatistics, object)
    */
  def prefix[K, S <: StatisticsAggregator[S]]
  (rdd: RDD[T], cmpKey: T => K, statsAgg: T => S)(implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] = {
    val sortedRdd = teraSorted(rdd, cmpKey).persist()
    val distPartitionStatistics = Utils.sendToAllHigherMachines(sc, partitionStatistics(sortedRdd, statsAgg).collect().zip(List.range(1, numPartitions)), numPartitions)

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
    rdd.mapPartitions(partitionIt => {
      if (partitionIt.isEmpty) {
        Iterator()
      } else {
        val start = partitionIt.next
        Iterator(partitionIt.foldLeft(statsAgg(start)){(acc, o) => acc.merge(statsAgg(o))})
      }
    })
  }
}
