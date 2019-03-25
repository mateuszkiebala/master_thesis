package minimal_algorithms

import minimal_algorithms.semi_join.{SemiJoinObject, SemiJoinSetTypeEnum}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Class implementing semi join algorithm.
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  */
class MinimalSemiJoin[T <: SemiJoinObject[T] : ClassTag](spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithm[T](spark, numOfPartitions) {

  /**
    * Imports two sets: R and T from the same domain.
    * @param rddR Set from which we report objects.
    * @param rddT Set in which we look for matches.
    * @return this
    */
  def importObjects(rddR: RDD[T], rddT: RDD[T]): this.type = {
    super.importObjects(rddR.union(rddT))
    this
  }

  /**
    * Applies semi join algorithm on imported data.
    * @return RDD of objects that belong to set R and have a match in set T.
    */
  def execute: RDD[T] = {
    val rdd = perfectlySorted(this.objects)
    val TBounds = sc.broadcast(rdd.mapPartitions(partition => {
      val tObjects = partition.filter(o => o.getSetType == SemiJoinSetTypeEnum.TType).toList
      if (tObjects.nonEmpty) Iterator(tObjects.min.getKey, tObjects.max.getKey) else Iterator.empty
    }).collect().toSet)

    rdd.mapPartitions(partition => {
      val groupedByType = partition.toList.groupBy(o => o.getSetType)
      if (groupedByType.contains(SemiJoinSetTypeEnum.RType)) {
        val rObjects = groupedByType(SemiJoinSetTypeEnum.RType)
        val tObjects = if (groupedByType.contains(SemiJoinSetTypeEnum.TType)) {
          groupedByType(SemiJoinSetTypeEnum.TType).map{o => o.getKey}.toSet.union(TBounds.value)
        } else {
          TBounds.value
        }
        rObjects.filter(o => tObjects.contains(o.getKey)).toIterator
      } else {
        Iterator.empty
      }
    })
  }
}
