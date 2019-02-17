package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

class MinimalSlidingAggregation[T <: KeyWeightedMAO[T] : ClassTag](spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithmWithKey[T](spark, numOfPartitions) {

  def computeWindowValues(rdd: RDD[(Int, T)], itemsCntByPartition: Int, windowLen: Int,
                          partitionsPrefixWeights: List[Int]): RDD[(Int, Int)] = {
    rdd.mapPartitionsWithIndex((index, partition) => {
      val pIndex = index + 1
      val pEleMinRank = index * itemsCntByPartition
      val pEleMaxRank = pIndex * itemsCntByPartition - 1
      val partitionObjects = partition.toList.sorted
      val baseObjects = partitionObjects.filter {case (rank, _) => rank >= pEleMinRank && rank <= pEleMaxRank}
      val prefixWeights = ((-1) :: partitionObjects.map {case (rank, _) => rank})
        .zip(partitionObjects.scanLeft(0)((result, rankMaoPair) => result + rankMaoPair._2.getWeight)).toMap
      baseObjects.map {case (rank, mao) => {
        val a = (rank+1-windowLen+itemsCntByPartition) / itemsCntByPartition
        val alpha = if (a >= 0) a else 0
        val (w2, maxRank) = if (pIndex > alpha && alpha > 0) {
          (partitionsPrefixWeights(pIndex-1)-partitionsPrefixWeights(alpha), alpha*itemsCntByPartition-1)
        } else if (alpha == 0) {
          (partitionsPrefixWeights(pIndex-1), -1)
        } else {
          (0, rank)
        }
        val minRank = if ((rank - windowLen + 1) < 0) 0 else rank - windowLen + 1
        val w1 = prefixWeights(maxRank) - prefixWeights(minRank-1)
        val w3 = if (alpha == pIndex) 0 else prefixWeights(rank) - prefixWeights(pEleMinRank) + baseObjects.head._2.getWeight
        (mao.getKey, w1 + w2 + w3)
      }}.toIterator
    })
  }

  def execute(input: RDD[T], windowLength: Int): RDD[(Int, Int)] = {
    val dataWithRanks = importObjects(input).perfectlySortedWithRanks
    val distributedData = distributeDataToRemotelyRelevantPartitions(dataWithRanks, windowLength)
    val prefixedWeights = spark.sparkContext.broadcast(getPartitionsWeights(dataWithRanks).collect().scanLeft(0)(_ + _).toList).value
    computeWindowValues(distributedData, this.itemsCntByPartition, windowLength, prefixedWeights)
  }

  private[this] def getPartitionsWeights(rdd: RDD[(Int, T)]): RDD[Int] = {
    rdd.mapPartitions(partition =>
      Iterator(partition.toList.foldLeft(0){(acc, p) => acc + p._2.getWeight})
    )
  }
}

