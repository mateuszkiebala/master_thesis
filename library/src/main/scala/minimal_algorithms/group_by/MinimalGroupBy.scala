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
  * @tparam T T <: GroupByObject[T] : ClassTag
  */
class MinimalGroupBy[T <: GroupByObject : ClassTag]
  (spark: SparkSession, numOfPartitions: Int) extends StatisticsMinimalAlgorithm[GroupByObject](spark, numOfPartitions) {

  def execute: RDD[(GroupByKey, StatisticsAggregator)] = {
    val masterIndex = 0
    perfectSort.mapPartitionsWithIndex((pIndex, partition) => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val grouped = partition.toList.groupBy(o => o.getKey)
        val minKey = grouped.keys.min
        val maxKey = grouped.keys.max
        grouped.map{ case (key, values) => {
          val destMachine = if (key == minKey || key == maxKey) masterIndex else pIndex
          val statsAggObject = if (values.isEmpty) {
            null.asInstanceOf[StatisticsAggregator]
          } else {
            values.tail.foldLeft(values.head.getAggregator){(res, o) => safeMerge(res, o.getAggregator)}
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
                null.asInstanceOf[StatisticsAggregator]
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
