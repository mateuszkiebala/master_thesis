package minimal_algorithms.sliding_aggregation

import minimal_algorithms.statistics.StatisticsAggregator
import minimal_algorithms.{MinimalAlgorithm, RangeTree, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import minimal_algorithms.statistics.StatisticsUtils.{safeMerge, partitionStatistics}

import scala.reflect.ClassTag

/**
  * Class implementing sliding aggregation algorithm.
  * @param spark  SparkSession
  * @param numPartitions  Number of partitions
  */
class MinimalSlidingAggregation(spark: SparkSession, numPartitions: Int) extends MinimalAlgorithm(spark, numPartitions) {

  def aggregate[T, K, S <: StatisticsAggregator[S]]
  (rdd: RDD[T], windowLength: Int, cmpKey: T => K, statsAgg: T => S, itemsCnt: Int = -1)
  (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(T, S)] = {
    val (itemsNoByPartition, rddItemsCount) = computeItemsNoByPartition(rdd, itemsCnt)
    val rankedData = perfectSortWithRanks(rdd, cmpKey, rddItemsCount).persist()
    val distData = distributeDataToRemotelyRelevantPartitions(rankedData, windowLength, itemsNoByPartition).persist()
    val distPartitionStatistics = partitionStatistics(rankedData.map{e => e._2}, statsAgg).collect().zipWithIndex
    val partitionsRangeTree = Utils.sendToAllMachines(sc, new RangeTree(distPartitionStatistics))
    windowValues(distData, windowLength, itemsNoByPartition, partitionsRangeTree, statsAgg)
  }

  private[this] def distributeDataToRemotelyRelevantPartitions[T]
  (rdd: RDD[(Int, T)], windowLength: Int, itemsNoByPartition: Int)(implicit ttag: ClassTag[T]): RDD[(Int, T)] = {
    val distNumPartitions = Utils.sendToAllMachines(sc, numPartitions)
    Utils.sendToMachinesBounded(rdd.mapPartitionsWithIndex((pIndex, partitionIt) => {
      if (windowLength <= itemsNoByPartition) {
        partitionIt.map{rankPair => (rankPair, List(pIndex, pIndex+1))}
      } else {
        val remRelM = (windowLength-1) / itemsNoByPartition
        partitionIt.map{rankPair => (rankPair, List(pIndex, pIndex+remRelM, pIndex+remRelM+1))}
      }
    }), distNumPartitions-1)
  }

  private[this] def windowValues[T, S <: StatisticsAggregator[S]]
  (rdd: RDD[(Int, T)], windowLength: Int, itemsNoByPartition: Int, partitionsRangeTree: RangeTree[S], statsAgg: T => S)
  (implicit ttag: ClassTag[T], stag: ClassTag[S]): RDD[(T, S)] = {
    rdd.mapPartitionsWithIndex((pIndex, partitionIt) => {
      val baseLowerBound = pIndex * itemsNoByPartition
      val baseUpperBound = (pIndex+1) * itemsNoByPartition - 1
      val partitionObjects = partitionIt.toList
      val baseObjects = partitionObjects.filter{case (rank, _) => rank >= baseLowerBound && rank <= baseUpperBound}
      val rankToIndex = partitionObjects.map{case (rank, _) => rank}.sorted.zipWithIndex.toMap
      val rangeTree = new RangeTree(partitionObjects.map{case (rank, o) => (statsAgg(o), rankToIndex(rank))})

      baseObjects.map{case (rank, o) => {
        val windowStart = if ((rank-windowLength+1) < 0) 0 else rank-windowLength+1
        val a = ((rank+1-windowLength+itemsNoByPartition) / itemsNoByPartition) - 1
        val alpha = if (a >= 0) a else -1
        val result = if (pIndex > alpha + 1) {
          val w1 = if (alpha < 0) null.asInstanceOf[S] else rangeTree.query(rankToIndex(windowStart), rankToIndex((alpha+1)*itemsNoByPartition-1))
          val w2 = partitionsRangeTree.query(alpha+1, pIndex-1)
          val w3 = rangeTree.query(rankToIndex(baseLowerBound), rankToIndex(rank))
          safeMerge(safeMerge(w1, w2), w3)
        } else {
          rangeTree.query(rankToIndex(windowStart), rankToIndex(rank))
        }
        (o, result)
      }}.toIterator
    })
  }
}