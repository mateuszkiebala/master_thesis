package minimal_algorithms.sliding_aggregation


import minimal_algorithms.statistics_aggregators.StatisticsAggregator
import minimal_algorithms.{RangeTree, StatisticsMinimalAlgorithm}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import minimal_algorithms.statistics_aggregators.Helpers.safeMerge
import scala.reflect.ClassTag

/**
  * Class implementing sliding aggregation algorithm.
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  * @tparam T T <: StatisticsMinimalAlgorithmObject[T] : ClassTag
  */

class MinimalSlidingAggregation[T <: Serializable](spark: SparkSession, numOfPartitions: Int)(implicit ttag: ClassTag[T])
  extends StatisticsMinimalAlgorithm[T](spark, numOfPartitions) {

  /**
    * Executes sliding aggregation algorithm for provided RDD
    * @param input  Initial RDD with objects.
    * @param windowLength Window length
    * @return RDD of pairs (object, sliding aggregation value)
    */
  def execute[K, S <: StatisticsAggregator[S]](input: RDD[T], windowLength: Int, cmpKey: T => K, statsAgg: T => S)
                (implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(T, S)] = {
    val dataWithRanks = importObjects(input).perfectSortWithRanks(cmpKey).persist()
    val distributedData = distributeDataToRemotelyRelevantPartitions(dataWithRanks, windowLength).persist()
    val elements = partitionsStatistics(dataWithRanks.map{e => e._2}, statsAgg).collect().zipWithIndex
    val partitionsRangeTree = spark.sparkContext.broadcast(new RangeTree(elements)).value
    computeWindowValues(distributedData, itemsCntByPartition, windowLength, partitionsRangeTree, statsAgg)
  }

  /**
    * Sends objects to remotely relevant partitions. Implements algorithm described in 'Minimal MapReduce Algorithms' paper.
    * @param rdd  RDD with objects to process.
    * @param windowLength  Window length.
    * @return RDD of pairs (ranking, object)
    */
  private[this] def distributeDataToRemotelyRelevantPartitions(rdd: RDD[(Int, T)], windowLength: Int): RDD[(Int, T)] = {
    val numOfPartitionsBroadcast = spark.sparkContext.broadcast(this.numOfPartitions).value
    val itemsCntByPartitionBroadcast = spark.sparkContext.broadcast(this.itemsCntByPartition).value
    sendToMachines[(Int, T)](rdd.mapPartitionsWithIndex((pIndex, partition) => {
      if (windowLength <= itemsCntByPartitionBroadcast) {
        partition.map {rankMaoPair =>
          if (pIndex+1 < numOfPartitionsBroadcast) {
            (rankMaoPair, List(pIndex, pIndex+1))
          } else {
            (rankMaoPair, List(pIndex))
          }
        }
      } else {
        val remRelM = (windowLength-1) / itemsCntByPartitionBroadcast
        partition.map {rankMaoPair =>
          if (pIndex+remRelM+1 < numOfPartitionsBroadcast) {
            (rankMaoPair, List(pIndex, pIndex+remRelM, pIndex+remRelM+1))
          } else if (pIndex+remRelM < numOfPartitionsBroadcast) {
            (rankMaoPair, List(pIndex, pIndex+remRelM))
          } else {
            (rankMaoPair, List(pIndex))
          }
        }
      }
    }))
  }

  private[this] def computeWindowValues[S <: StatisticsAggregator[S]]
    (rdd: RDD[(Int, T)], itemsCntByPartition: Int, windowLen: Int, partitionsRangeTree: RangeTree[S], statsAgg: T => S)
    (implicit stag: ClassTag[S]): RDD[(T, S)] = {
    rdd.mapPartitionsWithIndex((index, partition) => {
      val pEleMinRank = index * itemsCntByPartition
      val pEleMaxRank = (index + 1) * itemsCntByPartition - 1
      val partitionObjects = partition.toList

      val baseObjects = partitionObjects.filter{case (rank, _) => rank >= pEleMinRank && rank <= pEleMaxRank}
      val rankToIndex = partitionObjects.map{case (rank, _) => rank}.zipWithIndex.toMap
      val rangeTree = new RangeTree(partitionObjects.map{case (rank, o) => (statsAgg(o), rankToIndex(rank))}.toArray)

      baseObjects.map{case (rank, smao) => {
        val minRank = if ((rank - windowLen + 1) < 0) 0 else rank - windowLen + 1
        val a = ((rank+1-windowLen+itemsCntByPartition) / itemsCntByPartition) - 1
        val alpha = if (a >= 0) a else -1
        val result = if (index > alpha + 1) {
          val w1 = if (alpha < 0) null.asInstanceOf[S] else rangeTree.query(rankToIndex(minRank), rankToIndex((alpha+1)*itemsCntByPartition-1))
          val w2 = partitionsRangeTree.query(alpha+1, index-1)
          val w3 = rangeTree.query(rankToIndex(pEleMinRank), rankToIndex(rank))
          safeMerge(safeMerge(w1, w2), w3)
        } else {
          rangeTree.query(rankToIndex(minRank), rankToIndex(rank))
        }
        (smao, result)
      }}.toIterator
    })
  }
}
