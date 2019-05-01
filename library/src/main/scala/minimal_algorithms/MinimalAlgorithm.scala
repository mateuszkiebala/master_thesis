package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

/**
  * Class implementing base functions required to create a minimal algorithm.
  * @param spark  SparkSession object
  * @param numOfPartitions  Number of partitions.
  */
class MinimalAlgorithm[T](spark: SparkSession, numOfPartitions: Int)(implicit ttag: ClassTag[T]) {
  protected val sc = spark.sparkContext
  var objects: RDD[T] = sc.emptyRDD
  var itemsTotalCnt: Int = 0
  var itemsCntByPartition: Int = 0
  object PerfectPartitioner {}
  object KeyPartitioner {}

  /**
    * Imports objects.
    * @param rdd  Objects to be processed by minimal algorithm.
    * @return this
    */
  def importObjects(rdd: RDD[T], itemsTotalCnt: Int = -1): this.type = {
    this.objects = rdd.repartition(numOfPartitions)
    this.itemsTotalCnt = if (itemsTotalCnt < 0) this.objects.count().toInt else itemsTotalCnt
    this.itemsCntByPartition = computeItemsCntByPartition(this.objects, this.itemsTotalCnt)
    this
  }

  def computeItemsCntByPartition(rdd: RDD[T], itemsTotalCnt: Int = -1): Int = {
    val cnt = if (itemsTotalCnt < 0) rdd.count().toInt else itemsTotalCnt
    (cnt + this.numOfPartitions - 1) / this.numOfPartitions
  }

  /**
    * Applies Tera Sort algorithm on imported objects. Function affects imported objects.
    * @return this
    */
  def teraSort[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): this.type = {
    this.objects = this.objects.sortBy(cmpKey).persist()
    this
  }

  /**
    * Applies Tera Sort algorithm on provided RDD.
    * @param rdd  RDD on which Tera Sort will be performed.
    * @return Sorted RDD.
    */
  def teraSorted[K](rdd: RDD[T], cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    rdd.repartition(this.numOfPartitions).sortBy(cmpKey)
  }

  /**
    * Applies ranking algorithm on imported objects. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @return RDD of pairs (ranking, object)
    */
  def rank[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] =
    ranked(this.objects, cmpKey)

  /**
    * Applies ranking algorithm on given RDD. Order for equal objects is picked randomly.
    * Each object has unique ranking. Ranking starts from 0 index.
    * @param rdd  RDD with objects to process.
    * @return RDD of pairs (ranking, object)
    */
  def ranked[K](rdd: RDD[T], cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    val sortedRdd = teraSorted(rdd, cmpKey).persist()
    val distPartitionSizes = sendToAllHigherMachines(partitionSizes(sortedRdd).collect().zip(List.range(1, this.numOfPartitions)))
    sortedRdd.zipPartitions(distPartitionSizes){(partitionIt, partitionSizesIt) => {
      if (partitionIt.hasNext) {
        val offset = partitionSizesIt.sum
        partitionIt.zipWithIndex.map{case (o, index) => (index + offset, o)}
      } else {
        Iterator()
      }
    }}
  }

  /**
    * Sorts and perfectly balances imported objects. Function affects imported objects.
    * @return this
    */
  def perfectSort[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    this.objects = perfectlySortedWithRanks(this.objects, cmpKey, this.itemsTotalCnt).map(o => o._2).persist()
    this.objects
  }

  /**
    * Sorts and perfectly balances provided RDD.
    * @param rdd  RDD with objects to process.
    * @return Perfectly balanced RDD of objects
    */
  def perfectlySorted[K](rdd: RDD[T], cmpKey: T => K, itemsTotalCnt: Int = -1)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[T] = {
    perfectlySortedWithRanks(rdd, cmpKey, itemsTotalCnt).map(o => o._2)
  }

  /**
    * Sorts and perfectly balances imported objects.
    * @return Perfectly balanced RDD of pairs (ranking, object)
    */
  def perfectSortWithRanks[K](cmpKey: T => K)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    perfectlySortedWithRanks(this.objects, cmpKey, this.itemsTotalCnt)
  }

  /**
    * Sorts and perfectly balances provided RDD.
    * @param rdd  RDD with objects to process.
    * @return Perfectly balanced RDD of pairs (ranking, object)
    */
  def perfectlySortedWithRanks[K](rdd: RDD[T], cmpKey: T => K, itemsTotalCnt: Int = -1)(implicit ord: Ordering[K], ktag: ClassTag[K]): RDD[(Int, T)] = {
    ranked(rdd, cmpKey).partitionBy(new PerfectPartitioner(numOfPartitions, computeItemsCntByPartition(rdd, itemsTotalCnt)))
  }

  /**
    * Shuffles rdd objects by sending them to given machine indices.
    * @param rdd  RDD of pairs (object, List[destination machine index])
    * @tparam K [K : ClassTag]
    * @return RDD of reshuffled objects
    */
  def sendToMachines[R](rdd: RDD[(R, Seq[Int])])(implicit rtag: ClassTag[R]): RDD[R] = {
    partitionByKey[R](rdd.mapPartitionsWithIndex((pIndex, partition) => {
      partition.flatMap{case (o, indices) => if (indices.isEmpty) List((pIndex, o)) else indices.map{i => (i, o)}}
    }))
  }

  def sendToAllHigherMachines[R](arr: Seq[(R, Int)])(implicit rtag: ClassTag[R]): RDD[R] = {
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

  def sendToAllMachines[R](o: R)(implicit rtag: ClassTag[R]): R = {
    sc.broadcast(o).value
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
  def partitionSizes[R](rdd: RDD[R])(implicit rtag: ClassTag[R]): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.length))
  }
}
