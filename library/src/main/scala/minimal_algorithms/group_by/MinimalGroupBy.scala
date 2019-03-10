package minimal_algorithms.group_by

import minimal_algorithms.aggregation_function._
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
  def sum: RDD[(Int, Double)] = {
    this.groupBy(new SumAggregation)
  }

  /**
    * Groups objects by key and computes average on each group.
    * @return RDD of pairs (group key, average of group objects)
    */
  def avg: RDD[(Int, Double)] = {
    this.groupBy(new AverageAggregation)
  }

  /**
    * Groups objects by key and computes minimum object from each group.
    * @return RDD of pairs (group key, minimum object from group)
    */
  def min: RDD[(Int, Double)] = {
    this.groupBy(new MinAggregation)
  }

  /**
    * Groups objects by key and computes maximum object from each group.
    * @return RDD of pairs (group key, maximum object from group)
    */
  def max: RDD[(Int, Double)] = {
    this.groupBy(new MaxAggregation)
  }

  private[this] def groupBy(aggFun: AggregationFunction): RDD[(Int, Double)] = {
    val masterIndex = 0
    perfectlySorted(this.objects).mapPartitionsWithIndex((pIndex, partition) => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val grouped = partition.toList.groupBy(o => o.getKey)
        val minKey = grouped.keys.min
        val maxKey = grouped.keys.max
        grouped.map { case (k, v) => {
          val destMachine = if (k == minKey || k == maxKey) masterIndex else pIndex
          val groupedObject = new GroupedObject(k, v.foldLeft(aggFun.defaultValue)((res, o) => aggFun.apply(res, o.getWeight)), v.length)
          (destMachine, groupedObject)
        }
        }(collection.breakOut).toIterator
      }
    }).partitionBy(new KeyPartitioner(this.numOfPartitions)).map(p => p._2)
      .mapPartitionsWithIndex((pIndex, partition) => {
        if (pIndex == masterIndex) {
          partition.toList
            .groupBy{ o => o.key }
            .map{ case (key, objects) => {
              val result = objects.foldLeft(new GroupedObject(key, aggFun.defaultValue, 0))((res, o) => {
                res.value = aggFun.apply(res.value, o.value)
                res.length += o.length
                res
              })
              (result.key, if (aggFun.average) result.value.toDouble / result.length else result.value)
            } }(collection.breakOut)
            .toIterator
        } else {
          partition.map{ o => (o.key, o.value.toDouble) }
        }
      }).persist()
  }
}

class GroupedObject(k: Int, v: Int, l: Int) extends Serializable {
  var value: Int = v
  var key: Int = k
  var length: Int = l
}
