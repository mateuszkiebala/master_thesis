package minimal_algorithms

import minimal_algorithms.semi_join.{SemiJoinObject, SemiJoinSetTypeEnum}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Class implementing semi join algorithm.
  * @param spark  SparkSession
  * @param numPartitions  Number of partitionIts
  */
class MinimalSemiJoin[T <: SemiJoinObject]
(spark: SparkSession, numPartitions: Int)(implicit ttag: ClassTag[T]) extends MinimalAlgorithm[T](spark, numPartitions) {

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
    val rdd = perfectSort(cmpKey)
    val tKeyBounds = Utils.sendToAllMachines(sc, rdd.mapPartitions(partitionIt => {
      val tObjects = partitionIt.filter(o => o.getSetType == SemiJoinSetTypeEnum.TType).toList
      if (tObjects.nonEmpty) Iterator(cmpKey(tObjects.head), cmpKey(tObjects.last)) else Iterator()
    }).collect().toSet)

    rdd.mapPartitions(partitionIt => {
      val (rObjects, tObjects) = partitionIt.partition(o => o.getSetType == SemiJoinSetTypeEnum.RType)
      if (rObjects.nonEmpty) {
        val tKeys = if (tObjects.nonEmpty) tObjects.map(cmpKey).toSet.union(tKeyBounds) else tKeyBounds
        rObjects.filter(ro => tKeys.contains(cmpKey(ro)))
      } else {
        Iterator.empty
      }
    })
  }
}
