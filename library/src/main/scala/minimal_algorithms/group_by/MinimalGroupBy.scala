package minimal_algorithms.group_by

import minimal_algorithms.{KeyPartitioner, MinimalAlgorithmObjectWithKey, MinimalAlgorithmWithKey}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Class implementing aggregation functions.
  * @param spark  SparkSession
  * @param numOfPartitions  Number of partitions
  * @tparam T T <: MinimalAlgorithmObjectWithKey[T] : ClassTag
  */
class MinimalGroupBy[T <: MinimalAlgorithmObjectWithKey[T] : ClassTag](spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithmWithKey[T](spark, numOfPartitions) {

  /**
    * Groups objects by key and computes sum on each group.
    * @return RDD of pairs (group key, sum of group objects)
    */
  def groupBySum: RDD[(Int, Int)] = {
    this.groupBy(0, (x: Int, y: Int) => x + y)
  }

  /**
    * Groups objects by key and computes minimum object from each group.
    * @return RDD of pairs (group key, minimum object from group)
    */
  def groupByMin: RDD[(Int, Int)] = {
    this.groupBy(Int.MaxValue, (x: Int, y: Int) => math.min(x, y))
  }

  /**
    * Groups objects by key and computes maximum object from each group.
    * @return RDD of pairs (group key, maximum object from group)
    */
  def groupByMax: RDD[(Int, Int)] = {
    this.groupBy(Int.MinValue, (x: Int, y: Int) => math.max(x, y))
  }

  private[this] def groupBy(startEle: Int, fun: (Int, Int) => Int): RDD[(Int, Int)] = {
    val masterIndex = 0
    teraSorted(this.objects).mapPartitionsWithIndex((pIndex, partition) => {
      val grouped = partition.toList.groupBy(o => o.getKey)
      val minKey = grouped.keys.min
      val maxKey = grouped.keys.max
      grouped.map{ case (k, v) => {
        (if (k == minKey || k == maxKey) masterIndex else pIndex, (k, v.foldLeft(startEle)((res, o) => fun(res, o.getWeight))))
      }}(collection.breakOut).toIterator
    }).partitionBy(new KeyPartitioner(this.numOfPartitions)).map(p => p._2)
      .mapPartitionsWithIndex((pIndex, partition) => {
        if (pIndex == masterIndex) {
          partition.toList
            .groupBy{ case (k, _) => k }
            .map{ case (k, v) => (k, v.foldLeft(startEle)((res, p) => fun(res, p._2))) }(collection.breakOut)
            .toIterator
        } else {
          partition
        }
      }).persist()
  }
}
