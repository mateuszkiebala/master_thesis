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
  def sumGroupBy: RDD[(Int, Double)] = {
    this.groupBy(0, (x: Int, y: Int) => x + y)
  }

  /**
    * Groups objects by key and computes average on each group.
    * @return RDD of pairs (group key, average of group objects)
    */
  def averageGroupBy: RDD[(Int, Double)] = {
    this.groupBy(0, (x: Int, y: Int) => x + y, true)
  }

  /**
    * Groups objects by key and computes minimum object from each group.
    * @return RDD of pairs (group key, minimum object from group)
    */
  def minGroupBy: RDD[(Int, Double)] = {
    this.groupBy(Int.MaxValue, (x: Int, y: Int) => math.min(x, y))
  }

  /**
    * Groups objects by key and computes maximum object from each group.
    * @return RDD of pairs (group key, maximum object from group)
    */
  def maxGroupBy: RDD[(Int, Double)] = {
    this.groupBy(Int.MinValue, (x: Int, y: Int) => math.max(x, y))
  }

  private[this] def groupBy(startEle: Int, aggFun: (Int, Int) => Int, averageResult: Boolean = false): RDD[(Int, Double)] = {
    val masterIndex = 0
    teraSorted(this.objects).mapPartitionsWithIndex((pIndex, partition) => {
      val grouped = partition.toList.groupBy(o => o.getKey)
      val minKey = grouped.keys.min
      val maxKey = grouped.keys.max
      grouped.map{ case (k, v) => {
        (if (k == minKey || k == maxKey) masterIndex else pIndex, new GroupByObject(k, v.foldLeft(startEle)((res, o) => aggFun(res, o.getWeight)), v.length))
      }}(collection.breakOut).toIterator
    }).partitionBy(new KeyPartitioner(this.numOfPartitions)).map(p => p._2)
      .mapPartitionsWithIndex((pIndex, partition) => {
        if (pIndex == masterIndex) {
          partition.toList
            .groupBy{ o => o.key }
            .map{ case (key, objects) => {
              val result = objects.foldLeft(new GroupByObject(key, startEle, 0))((res, o) => {
                res.value = aggFun(res.value, o.value)
                res.length += o.length
                res
              })
              (result.key, if (averageResult) result.value.toDouble / result.length else result.value)
            } }(collection.breakOut)
            .toIterator
        } else {
          partition.map{ o => (o.key, o.value.toDouble) }
        }
      }).persist()
  }
}

class GroupByObject(k: Int, v: Int, l: Int) extends Serializable {
  var value: Int = v
  var key: Int = k
  var length: Int = l
}
