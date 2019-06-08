package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
import minimal_algorithms.statistics.StatisticsAggregator
import minimal_algorithms.group_by.MinimalGroupBy
import minimal_algorithms.sliding_aggregation.MinimalSlidingAggregation

class MinimalAlgFactory[T](spark: SparkSession, numPartitions: Int, input: RDD[T], itemsCnt: Int = -1)(implicit ttag: ClassTag[T]) {

  def teraSort[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    new MinimalAlgorithm(spark, numPartitions).teraSort(input, cmpKey);
  }

  def rank[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    new MinimalAlgorithm(spark, numPartitions).rank(input, cmpKey);
  }

  def perfectSort[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    new MinimalAlgorithm(spark, numPartitions).perfectSort(input, cmpKey, itemsCnt);
  }

  def perfectSortWithRanks[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    new MinimalAlgorithm(spark, numPartitions).perfectSortWithRanks(input, cmpKey, itemsCnt);
  }

  def prefix[K, S <: StatisticsAggregator[S]](cmpKey: T => K, statsAgg: T => S)
      (implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] = {
    new MinimalAlgorithm(spark, numPartitions).prefix(input, cmpKey, statsAgg);
  }

  def semiJoin[K](rddT: RDD[T], cmpKey: T => K, isRType: T => Boolean)
      (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[T] = {
    new MinimalSemiJoin(spark, numPartitions).semiJoin(input, rddT, cmpKey, isRType, itemsCnt)
  }

  def groupBy[K, S <: StatisticsAggregator[S]](cmpKey: T => K, statsAgg: T => S)
      (implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(K, S)] = {
    new MinimalGroupBy(spark, numPartitions).groupBy(input, cmpKey, statsAgg, itemsCnt);
  }

  def slidingAggregation[K, S <: StatisticsAggregator[S]](windowLength: Int, cmpKey: T => K, statsAgg: T => S)
      (implicit ord: Ordering[K], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(T, S)] = {
    new MinimalSlidingAggregation(spark, numPartitions).aggregate(input, windowLength, cmpKey, statsAgg, itemsCnt)
  }
}
