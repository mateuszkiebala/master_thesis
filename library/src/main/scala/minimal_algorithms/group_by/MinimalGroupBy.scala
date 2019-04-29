package minimal_algorithms.group_by

import minimal_algorithms.statistics_aggregators._
import minimal_algorithms.{KeyPartitioner, StatisticsMinimalAlgorithm}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import minimal_algorithms.statistics_aggregators.Helpers.safeMerge
import scala.reflect.ClassTag

/**
  * Class implementing group by algorithm.
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  */
class MinimalGroupBy[T <: Serializable](spark: SparkSession, numOfPartitions: Int)(implicit ttag: ClassTag[T])
  extends StatisticsMinimalAlgorithm[T](spark, numOfPartitions) {

  def execute[K, S <: StatisticsAggregator[S]](cmpKey: T => K, statsAgg: T => S)
                (implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(K, S)] = {
    val masterIndex = 0
    perfectSort(cmpKey).mapPartitionsWithIndex((pIndex, partition) => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val grouped = partition.toList.groupBy(cmpKey)
        val minKey = grouped.keys.min
        val maxKey = grouped.keys.max
        grouped.map{ case (key, values) => {
          val destMachine = if (key == minKey || key == maxKey) masterIndex else pIndex
          val statsAggObject = if (values.isEmpty) {
            null.asInstanceOf[S]
          } else {
            values.tail.foldLeft(statsAgg(values.head)){(res, o) => safeMerge(res, statsAgg(o))}
          }
          (destMachine, new GroupByObject(statsAggObject, key))
        }
        }(collection.breakOut).toIterator
      }
    }).partitionBy(new KeyPartitioner(this.numOfPartitions)).map(p => p._2)
      .mapPartitionsWithIndex((pIndex, partition) => {
        if (pIndex == masterIndex) {
          partition.toList
            .groupBy{o => o.getKey}
            .map{ case (key, values) => {
              val resultStatsAgg = if (values.isEmpty) {
                null.asInstanceOf[S]
              } else {
                values.tail.foldLeft(values.head.getAggregator){(res, o) => safeMerge(res, o.getAggregator)}
              }
              (key, resultStatsAgg)
            } }(collection.breakOut)
            .toIterator
        } else {
          partition.map{o => (o.getKey, o.getAggregator)}
        }
      }).persist()
  }
}

class GroupByObject[K, S <: StatisticsAggregator[S]](aggregator: S, key: K) extends Serializable {
  def getAggregator: S = this.aggregator
  def getKey: K = this.key
}
