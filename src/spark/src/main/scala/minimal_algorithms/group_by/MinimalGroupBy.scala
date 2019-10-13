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
  * @param numPartitions  Number of partitions. If you do not provide this value then algorithms will use default RDD partitioning.
  */
class MinimalGroupBy(spark: SparkSession, numPartitions: Int = -1) extends MinimalAlgorithm(spark, numPartitions) {

  /**
    * Runs group by algorithm on imported data.
    * @param rdd RDD of objects
    * @param cmpKey function to compare objects of rdd
    * @param statsAgg  function to compute statistics on the object of rdd
    * @param itemsCount number of items in rdd. If you do not provide this value then it will be computed.
    * @return groupped statistics for rdd.
    */
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
