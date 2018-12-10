package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class MinimalSemiJoin(spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithmWithKey[MySemiJoinType](spark, numOfPartitions) {

  def importObjects(rddR: RDD[MySemiJoinType], rddT: RDD[MySemiJoinType]): this.type = {
    this.objects = rddR.union(rddT).asInstanceOf[RDD[MinimalAlgorithmObject[MySemiJoinType]]]
    this
  }

  def execute: RDD[MySemiJoinType] = {
    val bounds = sc.broadcast(this.objects.asInstanceOf[RDD[MySemiJoinType]].mapPartitions(partition => {
      val tKeys = partition.filter(o => o.getSetType == MySemiJoinType.TType).toList
      if (tKeys.nonEmpty)
        Iterator(tKeys.min.getKey, tKeys.max.getKey)
      else
        Iterator.empty
    }).collect().toSet)

    this.objects.asInstanceOf[RDD[MySemiJoinType]].mapPartitionsWithIndex((pIndex, partition) => {
      val groupedByType = partition.toList.groupBy(o => o.getSetType)
      val rObjects = groupedByType(MySemiJoinType.RType)
      if (groupedByType.contains(MySemiJoinType.TType)) {
        val tKeys = groupedByType(MySemiJoinType.TType).map(o => o.getKey).toSet.union(bounds.value)
        rObjects.filter(o => tKeys.contains(o.getKey)).toIterator
      } else {
        Iterator.empty
      }
    })
  }
}
