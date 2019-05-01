package minimal_algorithms.sliding_aggregation

import minimal_algorithms.statistics_aggregators.StatisticsAggregator
import minimal_algorithms.{RangeTree, StatisticsMinimalAlgorithm}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import minimal_algorithms.statistics_aggregators.StatisticsUtils.safeMerge
import scala.reflect.ClassTag

/**
  * Class implementing sliding aggregation algorithm.
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  */
class MinimalSlidingAggregation[T]
(spark: SparkSession, numOfPartitions: Int)(implicit ttag: ClassTag[T])
  extends StatisticsMinimalAlgorithm[T](spark, numOfPartitions) {

  def execute[K, S <: StatisticsAggregator[S]]
  (windowLength: Int, cmpKey: T => K, statsAgg: T => S)(implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(T, S)] = {
    val rankedData = perfectSortWithRanks(cmpKey).persist()
    val distData = distributeDataToRemotelyRelevantPartitions(rankedData, windowLength).persist()
    val distPartitionStatistics = partitionStatistics(rankedData.map{e => e._2}, statsAgg).collect().zipWithIndex
    val partitionsRangeTree = sendToAllMachines(new RangeTree(distPartitionStatistics))
    windowValues(distData, windowLength, partitionsRangeTree, statsAgg)
  }

  private[this] def distributeDataToRemotelyRelevantPartitions(rdd: RDD[(Int, T)], windowLength: Int): RDD[(Int, T)] = {
    val distNumOfPartitions = sendToAllMachines(numOfPartitions)
    val distItemsCntByPartition = sendToAllMachines(itemsCntByPartition)
    sendToMachines(rdd.mapPartitionsWithIndex((pIndex, partitionIt) => {
      if (windowLength <= distItemsCntByPartition) {
        partitionIt.map{rankPair =>
          val machineIndices = if (pIndex+1 < distNumOfPartitions) List(pIndex, pIndex+1) else List(pIndex)
          (rankPair, machineIndices)
        }
      } else {
        val remRelM = (windowLength-1) / distItemsCntByPartition
        partitionIt.map{rankPair =>
          val machineIndices =
          if (pIndex+remRelM+1 < distNumOfPartitions) {
            List(pIndex, pIndex+remRelM, pIndex+remRelM+1)
          } else if (pIndex+remRelM < distNumOfPartitions) {
            List(pIndex, pIndex+remRelM)
          } else {
            List(pIndex)
          }
          (rankPair, machineIndices)
        }
      }
    }))
  }

  private[this] def windowValues[S <: StatisticsAggregator[S]]
  (rdd: RDD[(Int, T)], windowLength: Int, partitionsRangeTree: RangeTree[S], statsAgg: T => S)
  (implicit stag: ClassTag[S]): RDD[(T, S)] = {

    val distItemsCntByPartition = sendToAllMachines(itemsCntByPartition)
    rdd.mapPartitionsWithIndex((pIndex, partitionIt) => {
      val baseLowerBound = pIndex * distItemsCntByPartition
      val baseUpperBound = (pIndex+1) * distItemsCntByPartition - 1
      val partitionObjects = partitionIt.toList
      val baseObjects = partitionObjects.filter{case (rank, _) => rank >= baseLowerBound && rank <= baseUpperBound}
      val rankToIndex = partitionObjects.map{case (rank, _) => rank}.sorted.zipWithIndex.toMap
      val rangeTree = new RangeTree(partitionObjects.map{case (rank, o) => (statsAgg(o), rankToIndex(rank))})

      baseObjects.map{case (rank, o) => {
        val windowStart = if ((rank-windowLength+1) < 0) 0 else rank-windowLength+1
        val a = ((rank+1-windowLength+distItemsCntByPartition) / distItemsCntByPartition) - 1
        val alpha = if (a >= 0) a else -1
        val result = if (pIndex > alpha + 1) {
          val w1 = if (alpha < 0) null.asInstanceOf[S] else rangeTree.query(rankToIndex(windowStart), rankToIndex((alpha+1)*distItemsCntByPartition-1))
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
