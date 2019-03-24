package minimal_algorithms.sliding_aggregation

import minimal_algorithms.statistics_aggregators.StatisticsAggregator
import minimal_algorithms.{KeyPartitioner, RangeTree, StatisticsMinimalAlgorithm, StatisticsMinimalAlgorithmObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import minimal_algorithms.statistics_aggregators.Helpers.safeMerge
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class MinimalSlidingAggregation[A <: StatisticsAggregator[A], T <: StatisticsMinimalAlgorithmObject[T, A] : ClassTag : TypeTag]
  (spark: SparkSession, numOfPartitions: Int) extends StatisticsMinimalAlgorithm[A, T](spark, numOfPartitions) {

  /**
    * Computes sliding aggregation values for provided RDD and aggregation function.
    * @param input  Initial RDD with objects.
    * @param windowLength Window length
    * @param aggFun  Aggregation function
    * @return RDD of pairs (object's key, sliding aggregation value)
    */
  def execute(input: RDD[T], windowLength: Int)(implicit tag: ClassTag[A]): RDD[(T, A)] = {
    val dataWithRanks = importObjects(input).perfectlySortedWithRanks.persist()
    val distributedData = distributeDataToRemotelyRelevantPartitions(dataWithRanks, windowLength).persist()
    val elements = getPartitionsStatistics(dataWithRanks.map{e => e._2}).collect().zipWithIndex
    val partitionsRangeTree = spark.sparkContext.broadcast(new RangeTree[A](elements)).value
    computeWindowValues(distributedData, itemsCntByPartition, windowLength, partitionsRangeTree)
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
    rdd.mapPartitionsWithIndex((pIndex, partition) => {
      if (windowLength <= itemsCntByPartitionBroadcast) {
        partition.flatMap {rankMaoPair =>
          if (pIndex+1 < numOfPartitionsBroadcast) {
            List((pIndex, rankMaoPair), (pIndex+1, rankMaoPair))
          } else {
            List((pIndex, rankMaoPair))
          }
        }
      } else {
        val remRelM = (windowLength-1) / itemsCntByPartitionBroadcast
        partition.flatMap {rankMaoPair =>
          if (pIndex+remRelM+1 < numOfPartitionsBroadcast) {
            List((pIndex, rankMaoPair), (pIndex+remRelM, rankMaoPair), (pIndex+remRelM+1, rankMaoPair))
          } else if (pIndex+remRelM < numOfPartitionsBroadcast) {
            List((pIndex, rankMaoPair), (pIndex+remRelM, rankMaoPair))
          } else {
            List((pIndex, rankMaoPair))
          }
        }
      }
    }).partitionBy(new KeyPartitioner(this.numOfPartitions)).map(x => x._2)
  }

  def computeWindowValues(rdd: RDD[(Int, T)], itemsCntByPartition: Int, windowLen: Int,
                          partitionsRangeTree: RangeTree[A])(implicit tag: ClassTag[A]): RDD[(T, A)]= {
    rdd.mapPartitionsWithIndex((index, partition) => {
      val pEleMinRank = index * itemsCntByPartition
      val pEleMaxRank = (index + 1) * itemsCntByPartition - 1
      val partitionObjects = partition.toList.sorted

      val baseObjects = partitionObjects.filter{case (rank, _) => rank >= pEleMinRank && rank <= pEleMaxRank}
      val rankToIndex = partitionObjects.map{case (rank, _) => rank}.zipWithIndex.toMap
      val rangeTree = new RangeTree[A](partitionObjects.map{case (rank, o) => (o.getAggregator, rankToIndex(rank))}.toArray)

      baseObjects.map{case (rank, smao) => {
        val minRank = if ((rank - windowLen + 1) < 0) 0 else rank - windowLen + 1
        val a = ((rank+1-windowLen+itemsCntByPartition) / itemsCntByPartition) - 1
        val alpha = if (a >= 0) a else -1
        val result = if (index > alpha + 1) {
          val w1 = if (alpha < 0) null.asInstanceOf[A] else rangeTree.query(rankToIndex(minRank), rankToIndex((alpha+1)*itemsCntByPartition-1))
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
