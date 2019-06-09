package minimal_algorithms.spark.group_by

import minimal_algorithms.spark.statistics._
import minimal_algorithms.spark.{MinimalAlgorithm, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import minimal_algorithms.spark.statistics.StatisticsUtils._

import scala.reflect.ClassTag

/**
  * Class implementing group by algorithm.
  * @param spark  SparkSession
  * @param numPartitions  Number of partitions
  */
class MinimalGroupBy(spark: SparkSession, numPartitions: Int) extends MinimalAlgorithm(spark, numPartitions) {

  def groupBy[T, K, S <: StatisticsAggregator[S]]
  (rdd: RDD[T], cmpKey: T => K, statsAgg: T => S, elementsCnt: Int = -1)
  (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[(K, S)] = {
    val masterIndex = 0
    val mapPhase = perfectSort(rdd, cmpKey, elementsCnt).mapPartitionsWithIndex((pIndex, partitionIt) => {
      if (partitionIt.isEmpty) {
        Iterator[(GroupByObject[K, S], Seq[Int])]()
      } else {
        val grouped = partitionIt.toList.groupBy(cmpKey)
        val minKey = grouped.keys.min
        val maxKey = grouped.keys.max
        grouped.map{case (key, values) =>
          val destMachine = if (key == minKey || key == maxKey) masterIndex else pIndex
          (new GroupByObject(foldLeft(values.map{v => statsAgg(v)}), key), List(destMachine))
        }.toIterator
      }
    })

    Utils.sendToMachines(mapPhase).mapPartitionsWithIndex((pIndex, partitionIt) => {
      if (pIndex == masterIndex) {
        partitionIt.toList.groupBy{o => o.getKey}.map{case (key, values) =>
          (key, foldLeft(values.map{v => v.getAggregator}))
        }.toIterator
      } else {
        partitionIt.map{o => (o.getKey, o.getAggregator)}
      }
    })
  }
}

class GroupByObject[K, S <: StatisticsAggregator[S]](aggregator: S, key: K) extends Serializable {
  def getAggregator: S = aggregator
  def getKey: K = key
}
