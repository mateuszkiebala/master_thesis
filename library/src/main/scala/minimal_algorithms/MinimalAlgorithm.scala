package minimal_algorithms

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
    * Applies prefix sum algorithm on imported objects. First orders elements and then computes prefix sums.
    * Order for equal objects is picked randomly.
    * @return RDD of pairs (prefixSum, object)
    */
  def computeUniquePrefixSum: RDD[(Int, T)] = computeUniquePrefixSum(this.objects)

  /**
    * Applies prefix sum algorithm on provided RDD. First orders elements and then computes prefix sums.
    * Order for equal objects is picked randomly.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (prefixSum, object)
    */
  def computeUniquePrefixSum(rdd: RDD[T]): RDD[(Int, T)] = {
    val sortedRdd = teraSorted(rdd).persist()
    val prefixSumsOfObjects = sc.broadcast(getPartitionSums(sortedRdd).collect().scanLeft(0)(_ + _))
    sortedRdd.mapPartitionsWithIndex((index, partition) => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val maoObjects = partition.toList
        val prefSums = maoObjects.map(mao => mao.getWeight).scanLeft(prefixSumsOfObjects.value(index))(_+_).tail
        maoObjects.zipWithIndex.map{
          case (mao, i) => (prefSums(i), mao)
          case _        => throw new Exception("Error while creating prefix sums")
        }.toIterator
      }
    })
  }

  /**
    * Applies ranking algorithm on imported objects. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @return RDD of pairs (ranking, object)
    */
  def computeUniqueRanking: RDD[(Int, T)] = computeUniqueRanking(this.objects)

  /**
    * Applies ranking algorithm on given RDD. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (ranking, object)
    */
  def computeUniqueRanking(rdd: RDD[T]): RDD[(Int, T)] = {
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
    computeUniqueRanking(rdd).partitionBy(new PerfectPartitioner(numOfPartitions, this.itemsCntByPartition))
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

  private[this] def getPartitionSums(rdd: RDD[T]): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.map(o => o.getWeight).sum))
  }
}
