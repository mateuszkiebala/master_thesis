package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Class implementing semi join algorithm.
  * @param spark  SparkSession
  * @param numPartitions  Number of partitionIts
  */
class MinimalSemiJoin(spark: SparkSession, numPartitions: Int) extends MinimalAlgorithm(spark, numPartitions) {
  /**
    * Runs semi join algorithm on imported data.
    * @return RDD of objects that belong to set R and have a match in set T.
    */
  def semiJoin[T, K]
  (rddR: RDD[T], rddT: RDD[T], cmpKey: T => K, isRType: T => Boolean, itemsCount: Int = -1)
  (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[T] = {
    val rdd = perfectSort(rddR.union(rddT), cmpKey, itemsCount)
    val tKeyBounds = Utils.sendToAllMachines(sc, rdd.mapPartitions(partitionIt => {
      val tObjects = partitionIt.filterNot(isRType).toList
      if (tObjects.nonEmpty) Iterator(cmpKey(tObjects.head), cmpKey(tObjects.last)) else Iterator()
    }).collect().toSet)

    rdd.mapPartitions(partitionIt => {
      val (rObjects, tObjects) = partitionIt.partition(isRType)
      if (rObjects.nonEmpty) {
        val tKeys = if (tObjects.nonEmpty) tObjects.map(cmpKey).toSet.union(tKeyBounds) else tKeyBounds
        rObjects.filter(ro => tKeys.contains(cmpKey(ro)))
      } else {
        Iterator.empty
      }
    })
  }
}
