package minimal_algorithms

import minimal_algorithms.examples.{SemiJoinType, SemiJoinTypeEnum}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Class implementing semi join algorithm.
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  */
class MinimalSemiJoin(spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithmWithKey[SemiJoinType](spark, numOfPartitions) {

  /**
    * Imports two sets: R and T from the same domain.
    * @param rddR Set from which we report objects.
    * @param rddT Set in which we look for matches.
    * @return this
    */
  def importObjects(rddR: RDD[SemiJoinType], rddT: RDD[SemiJoinType]): this.type = {
    super.importObjects(rddR.union(rddT))
    this
  }

  /**
    * Applies semi join algorithm on imported data.
    * @return RDD of objects that belong to set R and have a match in set T.
    */
  def execute: RDD[SemiJoinType] = {
    val rdd = perfectlySorted(this.objects)
    val TBounds = sc.broadcast(rdd.mapPartitions(partition => {
      val tKeys = partition.filter(o => o.getSetType == SemiJoinTypeEnum.TType).toList
      if (tKeys.nonEmpty)
        Iterator(tKeys.min.getKey, tKeys.max.getKey)
      else
        Iterator.empty
    }).collect().toSet)

    rdd.mapPartitions(partition => {
      val groupedByType = partition.toList.groupBy(o => o.getSetType)
      if (groupedByType.contains(SemiJoinTypeEnum.RType)) {
        val rObjects = groupedByType(SemiJoinTypeEnum.RType)
        val tKeys = if (groupedByType.contains(SemiJoinTypeEnum.TType)) {
          groupedByType(SemiJoinTypeEnum.TType).map(o => o.getKey).toSet.union(TBounds.value)
        } else {
          TBounds.value
        }
        rObjects.filter(o => tKeys.contains(o.getKey)).toIterator
      } else {
        Iterator.empty
      }
    })
  }
}
