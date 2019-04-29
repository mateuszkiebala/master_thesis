package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
  * Class implementing base functions required to create a minimal algorithm.
  * @param spark  SparkSession object
  * @param numOfPartitions  Number of partitions.
  */
class MinimalAlgorithm[T <: Serializable](spark: SparkSession, numOfPartitions: Int)(implicit ttag: ClassTag[T]) {
  protected val sc = spark.sparkContext
  var objects: RDD[T] = sc.emptyRDD
  var itemsCntByPartition: Int = 0
  object PerfectPartitioner {}
  object KeyPartitioner {}

  /**
    * Imports objects.
    * @param rdd  Objects to be processed by minimal algorithm.
    * @return this
    */
  def importObjects(rdd: RDD[T]): this.type = {
    this.objects = rdd.repartition(numOfPartitions)
    itemsCntByPartition = (rdd.count().toInt+this.numOfPartitions-1) / this.numOfPartitions
    this
  }

  /**
    * Applies Tera Sort algorithm on imported objects. Function affects imported objects.
    * @return this
    */
  def teraSort[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): this.type = {
    this.objects = this.objects.sortBy(cmpKey).persist()
    this
  }

  /** Applies Tera Sort algorithm on provided RDD. Provided RDD will not be affected.
    * @param rdd  RDD on which Tera Sort will be performed.
    * @return Sorted RDD.
    */
  def teraSorted[K](rdd: RDD[T], cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    rdd.repartition(numOfPartitions).sortBy(cmpKey)
  }

  /**
    * Applies ranking algorithm on imported objects. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @return RDD of pairs (ranking, object)
    */
  def computeRanking[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = computeRanking(this.objects, cmpKey)

  /**
    * Applies ranking algorithm on given RDD. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (ranking, object)
    */
  def computeRanking[K](rdd: RDD[T], cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    val sortedRdd = teraSorted(rdd, cmpKey).persist()
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
  def perfectSort[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    this.objects = perfectlySortedWithRanks(this.objects, cmpKey).map(o => o._2).persist()
    this.objects
  }

  /**
    * Sorts and perfectly balances provided RDD.
    * @param rdd  RDD with objects to process.
    * @return Perfectly balanced RDD of objects
    */
  def perfectlySorted[K](rdd: RDD[T], cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    perfectlySortedWithRanks(rdd, cmpKey).map(o => o._2)
  }

  /**
    * Sorts and perfectly balances imported objects.
    * @return Perfectly balanced RDD of pairs (ranking, object)
    */
  def perfectlySortedWithRanks[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    perfectlySortedWithRanks(this.objects, cmpKey)
  }

  /**
    * Sorts and perfectly balances provided RDD.
    * @param rdd  RDD with objects to process.
    * @return Perfectly balanced RDD of pairs (ranking, object)
    */
  def perfectlySortedWithRanks[K](rdd: RDD[T], cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    computeRanking(rdd, cmpKey).partitionBy(new PerfectPartitioner(numOfPartitions, this.itemsCntByPartition))
  }

  /**
    * Shuffles rdd objects by sending them to given machine indices.
    * @param rdd  RDD of pairs (object, List[destination machine index])
    * @tparam K [K : ClassTag]
    * @return RDD of reshuffled objects
    */
  def sendToMachines[R](rdd: RDD[(R, List[Int])])(implicit rtag: ClassTag[R]): RDD[R] = {
    partitionByKey[R](rdd.mapPartitionsWithIndex((pIndex, partition) => {
      partition.flatMap{case (o, indices) => if (indices.isEmpty) List((pIndex, o)) else indices.map{i => (i, o)}}
    }))
  }

  def sendToAllHigherMachines[R](arr: Array[(R, Int)])(implicit rtag: ClassTag[R]): RDD[R] = {
    partitionByKey(sc.parallelize(arr.flatMap{case (o, lowerBound) => List.range(lowerBound, this.numOfPartitions).map{i => (i, o)}}))
  }

  def sendToAllHigherMachines[R](rdd: RDD[(R, Int)])(implicit rtag: ClassTag[R]): RDD[R] = {
    partitionByKey(rdd.mapPartitions(partition => {
      partition.flatMap{case (o, lowerBound) => List.range(lowerBound, this.numOfPartitions).map{i => (i, o)}}
    }))
  }

  def sendToAllLowerMachines[R](rdd: RDD[(R, Int)])(implicit rtag: ClassTag[R]): RDD[R] = {
    partitionByKey[R](rdd.mapPartitions(partition => {
      partition.flatMap{case (o, upperBound) => List.range(0, upperBound).map{i => (i, o)}}
    }))
  }

  def partitionByKey[R](rdd: RDD[(Int, R)])(implicit rtag: ClassTag[R]): RDD[R] = {
    rdd.partitionBy(new KeyPartitioner(this.numOfPartitions)).map(x => x._2)
  }

  /**
    * Returns number of elements on each partition.
    * @param rdd  RDD with objects to process.
    * @tparam R Type of RDD's objects.
    * @return Number of elements on each partition
    */
  def getPartitionSizes[R](rdd: RDD[R])(implicit rtag: ClassTag[R]): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.length))
  }
}
