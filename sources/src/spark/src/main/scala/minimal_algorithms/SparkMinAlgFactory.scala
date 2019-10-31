package minimal_algorithms.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
import minimal_algorithms.spark.statistics.StatisticsAggregator
import minimal_algorithms.spark.group_by.MinimalGroupBy
import minimal_algorithms.spark.sliding_aggregation.MinimalSlidingAggregation

class SparkMinAlgFactory(spark: SparkSession, numPartitions: Int = -1) {

  def teraSort[T, K](rdd: RDD[T], cmpKey: T => K, itemsCnt: Int = -1)
      (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[T] = {
    new MinimalAlgorithm(spark, numPartitions).teraSort(rdd, cmpKey);
  }

  def rank[T, K](rdd: RDD[T], cmpKey: T => K, itemsCnt: Int = -1)
      (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[(Int, T)] = {
    new MinimalAlgorithm(spark, numPartitions).rank(rdd, cmpKey);
  }

  def perfectSort[T, K](rdd: RDD[T], cmpKey: T => K, itemsCnt: Int = -1)
      (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[T] = {
    new MinimalAlgorithm(spark, numPartitions).perfectSort(rdd, cmpKey, itemsCnt);
  }

  def perfectSortWithRanks[T, K](rdd: RDD[T], cmpKey: T => K, itemsCnt: Int = -1)
      (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[(Int, T)] = {
    new MinimalAlgorithm(spark, numPartitions).perfectSortWithRanks(rdd, cmpKey, itemsCnt);
  }

  def prefix[T, K, S <: StatisticsAggregator[S]](rdd: RDD[T], cmpKey: T => K, statsAgg: T => S, itemsCnt: Int = -1)
      (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(S, T)] = {
    new MinimalAlgorithm(spark, numPartitions).prefix(rdd, cmpKey, statsAgg);
  }

  def semiJoin[T, K](rddR: RDD[T], rddT: RDD[T], cmpKey: T => K, isRType: T => Boolean, itemsCnt: Int = -1)
      (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K]): RDD[T] = {
    new MinimalSemiJoin(spark, numPartitions).semiJoin(rddR, rddT, cmpKey, isRType, itemsCnt)
  }

  def groupBy[T, K, S <: StatisticsAggregator[S]](rdd: RDD[T], cmpKey: T => K, statsAgg: T => S, itemsCnt: Int = -1)
      (implicit ord: Ordering[K], ttag: ClassTag[T], ktag: ClassTag[K], stag: ClassTag[S]): RDD[(K, S)] = {
    new MinimalGroupBy(spark, numPartitions).groupBy(rdd, cmpKey, statsAgg, itemsCnt);
  }

  def slidingAggregation[T, K, S <: StatisticsAggregator[S]](rdd: RDD[T], windowLength: Int, cmpKey: T => K, statsAgg: T => S, itemsCnt: Int = -1)
      (implicit ord: Ordering[K],ttag: ClassTag[T],  ktag: ClassTag[K], stag: ClassTag[S]): RDD[(T, S)] = {
    new MinimalSlidingAggregation(spark, numPartitions).aggregate(rdd, windowLength, cmpKey, statsAgg, itemsCnt)
  }
}
