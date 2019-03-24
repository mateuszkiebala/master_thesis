package minimal_algorithms

import minimal_algorithms.statistics_aggregators.StatisticsAggregator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

class StatisticsMinimalAlgorithm[A <: StatisticsAggregator[A], T <: StatisticsMinimalAlgorithmObject[T, A] : ClassTag]
  (spark: SparkSession, numOfPartitions: Int) extends MinimalAlgorithm[T](spark, numOfPartitions) {

  /**
    * Applies prefix aggregation (SUM, MIN, MAX) function on imported objects. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @param aggFun Aggregation function
    * @return RDD of pairs (prefixSum, object)
    */
  def computePrefix(implicit tag: ClassTag[A]): RDD[(A, T)] = computePrefix(this.objects)

  /**
    * Applies prefix aggregation (SUM, MIN, MAX) function on provided RDD. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @param rdd  RDD with objects to process.
    * @param aggFun Aggregation function
    * @return RDD of pairs (prefixValue, object)
    */
  def computePrefix(rdd: RDD[T])(implicit tag: ClassTag[A]): RDD[(A, T)] = {
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

  def getPrefixedPartitionStatistics(rdd: RDD[T])(implicit tag: ClassTag[A]): Array[A] = {
    val elements = getPartitionsStatistics(rdd).collect()
    if (elements.isEmpty) {
      Array()
    } else {
      elements.tail.scanLeft(elements.head){(res, a) => res.merge(a)}
    }
  }

  /**
    * Computes aggregated values (SUM, MIN, MAX) for each partition.
    * @param rdd  Elements
    * @param aggFun Aggregation function
    * @return RDD[aggregated value for partition]
    */
  def getPartitionsStatistics(rdd: RDD[T])(implicit tag: ClassTag[A]): RDD[A] = {
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
