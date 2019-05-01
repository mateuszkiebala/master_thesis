package minimal_algorithms.group_by

import minimal_algorithms.statistics_aggregators._
import minimal_algorithms.StatisticsMinimalAlgorithm
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import minimal_algorithms.statistics_aggregators.StatisticsUtils._
import scala.reflect.ClassTag

/**
  * Class implementing group by algorithm.
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  */
class MinimalGroupBy[T](spark: SparkSession, numOfPartitions: Int)(implicit ttag: ClassTag[T])
  extends StatisticsMinimalAlgorithm[T](spark, numOfPartitions) {

  def execute[K, S <: StatisticsAggregator[S]]
  (cmpKey: T => K, statsAgg: T => S)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(K, S)] = {
    val masterIndex = 0
    val mapPhase = perfectSort(cmpKey).mapPartitionsWithIndex((pIndex, partition) => {
      if (partition.isEmpty) {
        Iterator[(GroupByObject[K, S], Seq[Int])]()
      } else {
        val grouped = partition.toList.groupBy(cmpKey)
        val minKey = grouped.keys.min
        val maxKey = grouped.keys.max
        grouped.map{case (key, values) =>
          val destMachine = if (key == minKey || key == maxKey) masterIndex else pIndex
          (new GroupByObject(foldLeft(values.map{v => statsAgg(v)}), key), List(destMachine))
        }.toIterator
      }
    })

    sendToMachines(mapPhase).mapPartitionsWithIndex((pIndex, partition) => {
      if (pIndex == masterIndex) {
        partition.toList.groupBy{o => o.getKey}.map{case (key, values) =>
          (key, foldLeft(values.map{v => v.getAggregator}))
        }.toIterator
      } else {
        partition.map{o => (o.getKey, o.getAggregator)}
      }
    })
  }
}

class GroupByObject[K, S <: StatisticsAggregator[S]](aggregator: S, key: K) extends Serializable {
  def getAggregator: S = aggregator
  def getKey: K = key
}
