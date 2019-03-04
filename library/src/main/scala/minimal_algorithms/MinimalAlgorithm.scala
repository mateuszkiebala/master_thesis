package minimal_algorithms

import minimal_algorithms.aggregation_function.AggregationFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

/**
  * Class implementing base functions required to create a minimal algorithm.
  * @param spark  SparkSession object
  * @param numOfPartitions  Number of partitions.
  * @tparam T T <: MinimalAlgorithmObject[T] : ClassTag
  */
class MinimalAlgorithm[T <: MinimalAlgorithmObject[T] : ClassTag](spark: SparkSession, numOfPartitions: Int) {
  protected val sc = spark.sparkContext
  protected var objects: RDD[T] = sc.emptyRDD
  var itemsCntByPartition: Int = 0
  object PerfectPartitioner {}
  object KeyPartitioner {}

  /**
    * Imports objects.
    * @param rdd  Objects to be processed by minimal algorithm.
    * @return this
    */
  def importObjects(rdd: RDD[T]): this.type = {
    this.objects = rdd
    itemsCntByPartition = (rdd.count().toInt+this.numOfPartitions-1) / this.numOfPartitions
    this
  }

  /**
    * Applies Tera Sort algorithm on imported objects. Function affects imported objects.
    * @return this
    */
  def teraSort: this.type = {
    this.objects = teraSorted(this.objects).persist()
    this
  }

  /** Applies Tera Sort algorithm on provided RDD. Provided RDD will not be affected.
    * @param rdd  RDD on which Tera Sort will be performed.
    * @return Sorted RDD.
    */
  def teraSorted(rdd: RDD[T]): RDD[T] = {
    import spark.implicits._
    val pairedObjects = rdd.map{mao => (mao, mao)}
    pairedObjects.partitionBy(new RangePartitioner[T, T](numOfPartitions, pairedObjects))
      .sortByKey().values
  }

  /**
    * Applies prefix aggregation (SUM, MIN, MAX) function on imported objects. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @param aggFun Aggregation function
    * @return RDD of pairs (prefixSum, object)
    */
  def computePrefix(aggFun: AggregationFunction): RDD[(Int, T)] = computePrefix(this.objects, aggFun)

  /**
    * Applies prefix aggregation (SUM, MIN, MAX) function on provided RDD. First orders elements and then computes prefixes.
    * Order for equal objects is picked randomly.
    * @param rdd  RDD with objects to process.
    * @param aggFun Aggregation function
    * @return RDD of pairs (prefixValue, object)
    */
  def computePrefix(rdd: RDD[T], aggFun: AggregationFunction): RDD[(Int, T)] = {
    val sortedRdd = teraSorted(rdd).persist()
    val prefixPartitions = sc.broadcast(getPartitionsAggregatedWeights(sortedRdd, aggFun).collect()
      .scanLeft(aggFun.defaultValue)((res, x) => aggFun.apply(res, x)))
    sortedRdd.mapPartitionsWithIndex((index, partition) => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val maoObjects = partition.toList
        val prefix = maoObjects.map(mao => mao.getWeight).scanLeft(prefixPartitions.value(index))((res, x) => aggFun.apply(res, x)).tail
        maoObjects.zipWithIndex.map{
          case (mao, i) => (prefix(i), mao)
          case _        => throw new Exception("Error while creating prefix values")
        }.toIterator
      }
    })
  }

  /**
    * Applies ranking algorithm on imported objects. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @return RDD of pairs (ranking, object)
    */
  def computeRanking: RDD[(Int, T)] = computeRanking(this.objects)

  /**
    * Applies ranking algorithm on given RDD. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (ranking, object)
    */
  def computeRanking(rdd: RDD[T]): RDD[(Int, T)] = {
    val sortedRdd = teraSorted(rdd).persist()
    val prefixSumsOfPartitionSizes = sc.broadcast(getPartitionSizes(sortedRdd).collect().scanLeft(0)(_+_))
    sortedRdd.mapPartitionsWithIndex((index, partition) => {
      val offset = prefixSumsOfPartitionSizes.value(index)
      if (partition.isEmpty)
        Iterator()
      else
        partition.toList.zipWithIndex.map{
          case (mao, i) => (i+offset, mao)
          case _        => throw new Exception("Error while creating ranking")
        }.toIterator
    })
  }

  /**
    * Sorts and perfectly balances imported objects. Function affects imported objects.
    * @return this
    */
  def perfectSort: this.type = {
    this.objects = perfectlySortedWithRanks(this.objects).map(o => o._2).persist()
    this
  }

  /**
    * Sorts and perfectly balances imported objects.
    * @return Perfectly balanced RDD of pairs (ranking, object)
    */
  def perfectlySortedWithRanks: RDD[(Int, T)] = {
    perfectlySortedWithRanks(this.objects)
  }

  /**
    * Sorts and perfectly balances provided RDD.
    * @param rdd  RDD with objects to process.
    * @return Perfectly balanced RDD of pairs (ranking, object)
    */
  def perfectlySortedWithRanks(rdd: RDD[T]): RDD[(Int, T)] = {
    computeRanking(rdd).partitionBy(new PerfectPartitioner(numOfPartitions, this.itemsCntByPartition))
  }

  /**
    * Returns number of elements on each partition.
    * @param rdd  RDD with objects to process.
    * @tparam A Type of RDD's objects.
    * @return Number of elements on each partition
    */
  def getPartitionSizes[A](rdd: RDD[A]): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.length))
  }

  /**
    * Computes aggregated values (SUM, MIN, MAX) for each partition.
    * @param rdd  Elements
    * @param aggFun Aggregation function
    * @return RDD[aggregated value for partition]
    */
  def getPartitionsAggregatedWeights(rdd: RDD[T], aggFun: AggregationFunction): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.toList.foldLeft(aggFun.defaultValue){(acc, o) => aggFun.apply(acc, o.getWeight)}))
  }
}
