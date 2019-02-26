package minimal_algorithms.sliding_aggregation

import minimal_algorithms.{KeyPartitioner, MinimalAlgorithmObjectWithKey, MinimalAlgorithmWithKey, RangeTree}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Class implementing sliding aggregation algorithm. Currently works only for SUM.
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  * @tparam T T <: MinimalAlgorithmObjectWithKey[T] : ClassTag
  */
class MinimalSlidingAggregation[T <: MinimalAlgorithmObjectWithKey[T] : ClassTag](spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithmWithKey[T](spark, numOfPartitions) {

  def aggregateSum(input: RDD[T], windowLength: Int): RDD[(Int, Int)] = {
    execute(input, windowLength, (x: Int, y: Int) => x + y, 0)
  }

  def aggregateMin(input: RDD[T], windowLength: Int): RDD[(Int, Int)] = {
    execute(input, windowLength, (x: Int, y: Int) => math.min(x, y), Int.MaxValue)
  }

  def aggregateMax(input: RDD[T], windowLength: Int): RDD[(Int, Int)] = {
    execute(input, windowLength, (x: Int, y: Int) => math.max(x, y), Int.MinValue)
  }

  /**
    * Computes sliding aggregation values for provided RDD and aggregation function.
    * @param input  Initial RDD with objects.
    * @param windowLength Window length
    * @param aggFun  Aggragation function: sum, max, min
    * @return RDD of pairs (object's key, sliding aggregation value)
    */
  private[this] def execute(input: RDD[T], windowLength: Int, aggFun: (Int, Int) => Int, aggDefaultValue: Int): RDD[(Int, Int)] = {
    val dataWithRanks = importObjects(input).perfectlySortedWithRanks.persist()
    val distributedData = distributeDataToRemotelyRelevantPartitions(dataWithRanks, windowLength).persist()
    val elements = getPartitionsAggregatedWeights(dataWithRanks, aggFun).collect().zipWithIndex.toList
    val partitionsRangeTree = spark.sparkContext.broadcast(new RangeTree(elements, aggFun, aggDefaultValue)).value
    computeWindowValues(distributedData, itemsCntByPartition, windowLength, partitionsRangeTree, aggFun, aggDefaultValue)
  }

  /**
    * Sends objects to remotely relevant partitions. Implements algorithm described in 'Minimal MapReduce Algorithms' paper.
    * @param rdd  RDD with objects to process.
    * @param windowLength  Window length.
    * @return RDD of pairs (ranking, object)
    */
  private[this] def distributeDataToRemotelyRelevantPartitions(rdd: RDD[(Int, T)], windowLength: Int): RDD[(Int, T)] = {
    val numOfPartitionsBroadcast = sc.broadcast(this.numOfPartitions).value
    val itemsCntByPartitionBroadcast = sc.broadcast(this.itemsCntByPartition).value
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

  private[this] def getPartitionsAggregatedWeights(rdd: RDD[(Int, T)], fun: (Int, Int) => Int): RDD[Int] = {
    rdd.mapPartitions(partition =>
      Iterator(partition.toList.foldLeft(0){(acc, p) => fun(acc, p._2.getWeight)})
    )
  }

  private[this] def computeWindowValues(rdd: RDD[(Int, T)], itemsCntByPartition: Int, windowLen: Int,
                                        partitionsRangeTree: RangeTree, aggFun: (Int, Int) => Int, aggDefaultValue: Int): RDD[(Int, Int)] = {
    rdd.mapPartitionsWithIndex((index, partition) => {
      val pEleMinRank = index * itemsCntByPartition
      val pEleMaxRank = (index + 1) * itemsCntByPartition - 1
      val partitionObjects = partition.toList.sorted
      val baseObjects = partitionObjects.filter {case (rank, _) => rank >= pEleMinRank && rank <= pEleMaxRank}
      val rankToIndex= partitionObjects.map {case (rank, _) => rank}.zipWithIndex.toMap
      val rangeTree = new RangeTree(partitionObjects.map {case (rank, o) => (o.getWeight, rankToIndex(rank))}, aggFun, aggDefaultValue)

      baseObjects.map {case (rank, mao) => {
        val minRank = if ((rank - windowLen + 1) < 0) 0 else rank - windowLen + 1
        val a = ((rank+1-windowLen+itemsCntByPartition) / itemsCntByPartition) - 1
        val alpha = if (a >= 0) a else 0
        val result = if (index > alpha + 1) {
          val w1 = rangeTree.query(rankToIndex(minRank), rankToIndex((alpha+1)*itemsCntByPartition-1))
          val w2 = partitionsRangeTree.query(alpha+1, index-1)
          val w3 = rangeTree.query(rankToIndex(pEleMinRank), rankToIndex(rank))
          w1 + w2 + w3
        } else {
          rangeTree.query(rankToIndex(minRank), rankToIndex(rank))
        }
        (mao.getKey, result)
      }}.toIterator
    })
  }
}
