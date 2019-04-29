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
class MinimalSemiJoin[T <: SemiJoinObject](spark: SparkSession, numOfPartitions: Int)
                                          (implicit ttag: ClassTag[T]) extends MinimalAlgorithm[T](spark, numOfPartitions) {

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
    * Runs semi join algorithm on imported data.
    * @return RDD of objects that belong to set R and have a match in set T.
    */
  def execute[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    val rdd = perfectlySorted(this.objects, cmpKey)
    val TBounds = sc.broadcast(rdd.mapPartitions(partition => {
      val tObjects = partition.filter(o => o.getSetType == SemiJoinSetTypeEnum.TType).toList
      if (tObjects.nonEmpty) Iterator(cmpKey(tObjects.head), cmpKey(tObjects.last)) else Iterator.empty
    }).collect().toSet)

    rdd.mapPartitions(partition => {
      val groupedByType = partition.toList.groupBy(o => o.getSetType)
      if (groupedByType.contains(SemiJoinSetTypeEnum.RType)) {
        val rObjects = groupedByType(SemiJoinSetTypeEnum.RType)
        val tObjects = if (groupedByType.contains(SemiJoinSetTypeEnum.TType)) {
          groupedByType(SemiJoinSetTypeEnum.TType).map(cmpKey).toSet.union(TBounds.value)
        } else {
          TBounds.value
        }
        rObjects.filter(o => tObjects.contains(cmpKey(o))).toIterator
      } else {
        Iterator.empty
      }
    })
  }
}
