package minimal_algorithms

import org.apache.spark.rdd.RDD
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
  def teraSort: this.type = {
    this.objects = this.objects.sortBy(identity).persist()
    this
  }

  /** Applies Tera Sort algorithm on provided RDD. Provided RDD will not be affected.
    * @param rdd  RDD on which Tera Sort will be performed.
    * @return Sorted RDD.
    */
  def teraSorted(rdd: RDD[T]): RDD[T] = {
    rdd.repartition(numOfPartitions).sortBy(identity)
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
  def perfectSort: RDD[T] = {
    this.objects = perfectlySortedWithRanks(this.objects).map(o => o._2).persist()
    this.objects
  }

  /**
    * Sorts and perfectly balances provided RDD.
    * @param rdd  RDD with objects to process.
    * @return Perfectly balanced RDD of objects
    */
  def perfectlySorted(rdd: RDD[T]): RDD[T] = {
    perfectlySortedWithRanks(rdd).map(o => o._2)
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
    * Shuffles rdd objects by sending them to given machine indices.
    * @param rdd  RDD of pairs (object, List[destination machine index])
    * @tparam K [K : ClassTag]
    * @return RDD of reshuffled objects
    */
  def sendToMachines[K : ClassTag](rdd: RDD[(K, List[Int])]): RDD[K] = {
    rdd.mapPartitionsWithIndex((pIndex, partition) => {
      partition.flatMap { case (o, indices) => if (indices.isEmpty) List((pIndex, o)) else indices.map { i => (i, o) } }
    }).partitionBy(new KeyPartitioner(this.numOfPartitions)).map(x => x._2)
  }

  /**
    * Returns number of elements on each partition.
    * @param rdd  RDD with objects to process.
    * @tparam K Type of RDD's objects.
    * @return Number of elements on each partition
    */
  def getPartitionSizes[K : ClassTag](rdd: RDD[K]): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.length))
  }
}
