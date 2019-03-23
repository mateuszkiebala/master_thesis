package minimal_algorithms.sliding_aggregation

import minimal_algorithms.aggregations._
import minimal_algorithms.{KeyPartitioner, MinimalAlgorithmObjectWithKey, MinimalAlgorithmWithKey, RangeTree}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Class implementing sliding aggregation algorithms (SUM, AVG, MIN, MAX).
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  * @tparam T T <: MinimalAlgorithmObjectWithKey[T] : ClassTag
  */
class MinimalSlidingAggregation[T <: MinimalAlgorithmObjectWithKey[T] : ClassTag](spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithmWithKey[T](spark, numOfPartitions) {

  /**
    * Computes sliding sum values for provided RDD and window length.
    * @param input  RDD with input elements
    * @param windowLength window length
    * @return list of [(object o's key, sum of l largest objects not exceeding o)
    */
  /*def sum(input: RDD[T], windowLength: Int): RDD[(Int, Double)] = {
    //execute(input, windowLength, new SumAggregation)
  }

  /**
    * Computes sliding average values for provided RDD and window length.
    * @param input  RDD with input elements
    * @param windowLength window length
    * @return list of [(object o's key, average value of l largest objects not exceeding o)
    */
  def avg(input: RDD[T], windowLength: Int): RDD[(Int, Double)] = {
    execute(input, windowLength, new AverageAggregation)
  }

  /**
    * Computes sliding minimum values for provided RDD and window length.
    * @param input  RDD with input elements
    * @param windowLength window length
    * @return list of [(object o's key, minimum of l largest objects not exceeding o)
    */
  def min(input: RDD[T], windowLength: Int): RDD[(Int, Double)] = {
    execute(input, windowLength, new MinAggregation)
  }

  /**
    * Computes sliding maximum values for provided RDD and window length.
    * @param input  RDD with input elements
    * @param windowLength window length
    * @return list of [(object o's key, maximum of l largest objects not exceeding o)
    */
  def max(input: RDD[T], windowLength: Int): RDD[(Int, Double)] = {
    execute(input, windowLength, new MaxAggregation)
  }

  /**
    * Computes sliding aggregation values for provided RDD and aggregation function.
    * @param input  Initial RDD with objects.
    * @param windowLength Window length
    * @param aggFun  Aggregation function
    * @return RDD of pairs (object's key, sliding aggregation value)
    */
  private[this] def execute(input: RDD[T], windowLength: Int, aggFun: AggregationFunction): RDD[(Int, Double)] = {
    val dataWithRanks = importObjects(input).perfectlySortedWithRanks.persist()
    val distributedData = distributeDataToRemotelyRelevantPartitions(dataWithRanks, windowLength).persist()
    val elements = getPartitionsAggregatedWeights(dataWithRanks, aggFun).collect().zipWithIndex.toList
    //val partitionsRangeTree = spark.sparkContext.broadcast(new RangeTree(elements, aggFun)).value
    //computeWindowValues(distributedData, itemsCntByPartition, windowLength, partitionsRangeTree, aggFun)
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

  /**
    * Computes aggregated values for each partition.
    * @param rdd  Ranked elements
    * @param aggFun Aggregation function
    * @return RDD[aggregated value for partition]
    */
  private[this] def getPartitionsAggregatedWeights(rdd: RDD[(Int, T)], aggFun: AggregationFunction): RDD[Double] = {
    rdd.mapPartitions(partition =>
      Iterator(partition.toList.foldLeft(aggFun.defaultValue){(acc, p) => aggFun.apply(acc, p._2.getWeight)})
    )
  }

  private[this] def computeWindowValues(rdd: RDD[(Int, T)], itemsCntByPartition: Int, windowLen: Int,
                                        partitionsRangeTree: RangeTree, aggFun: AggregationFunction): RDD[(Int, Double)] = {
    rdd.mapPartitionsWithIndex((index, partition) => {
      val pEleMinRank = index * itemsCntByPartition
      val pEleMaxRank = (index + 1) * itemsCntByPartition - 1
      val partitionObjects = partition.toList.sorted
      val baseObjects = partitionObjects.filter {case (rank, _) => rank >= pEleMinRank && rank <= pEleMaxRank}
      val rankToIndex = partitionObjects.map {case (rank, _) => rank}.zipWithIndex.toMap
      val rangeTree = new RangeTree(partitionObjects.map {case (rank, o) => (o.getWeight, rankToIndex(rank))}, aggFun)

      baseObjects.map {case (rank, mao) => {
        val minRank = if ((rank - windowLen + 1) < 0) 0 else rank - windowLen + 1
        val a = ((rank+1-windowLen+itemsCntByPartition) / itemsCntByPartition) - 1
        val alpha = if (a >= 0) a else -1
        val result = if (index > alpha + 1) {
          val w1 = if (alpha < 0) 0 else rangeTree.query(rankToIndex(minRank), rankToIndex((alpha+1)*itemsCntByPartition-1))
          val w2 = partitionsRangeTree.query(alpha+1, index-1)
          val w3 = rangeTree.query(rankToIndex(pEleMinRank), rankToIndex(rank))
          aggFun.apply(aggFun.apply(w1, w2), w3)
        } else {
          rangeTree.query(rankToIndex(minRank), rankToIndex(rank))
        }
        (mao.getKey, if (aggFun.average) result.toDouble / (rank-minRank+1) else result)
      }}.toIterator
    })
  }*/
}
