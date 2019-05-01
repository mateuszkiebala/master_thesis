package minimal_algorithms

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Utils {
  object KeyPartitioner {}

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

  def sendToAllHigherMachines[R](sc: SparkContext, arr: Seq[(R, Int)], numPartitions: Int)(implicit rtag: ClassTag[R]): RDD[R] = {
    sendToAllHigherMachines(sc.parallelize(arr, numPartitions))
  }

  def sendToAllHigherMachines[R](rdd: RDD[(R, Int)])(implicit rtag: ClassTag[R]): RDD[R] = {
    val numPartitions = rdd.getNumPartitions
    partitionByKey(rdd.mapPartitions(partition => {
      partition.flatMap{case (o, lowerBound) => List.range(lowerBound, numPartitions).map{i => (i, o)}}
    }))
  }

  def sendToAllLowerMachines[R](sc: SparkContext, arr: Seq[(R, Int)], numPartitions: Int)(implicit rtag: ClassTag[R]): RDD[R] = {
    sendToAllLowerMachines(sc.parallelize(arr, numPartitions))
  }

  def sendToAllLowerMachines[R](rdd: RDD[(R, Int)])(implicit rtag: ClassTag[R]): RDD[R] = {
    partitionByKey[R](rdd.mapPartitions(partition => {
      partition.flatMap{case (o, upperBound) => List.range(0, upperBound).map{i => (i, o)}}
    }))
  }

  def sendToAllMachines[R](sc: SparkContext, o: R)(implicit rtag: ClassTag[R]): R = {
    sc.broadcast(o).value
  }

  def partitionByKey[R](rdd: RDD[(Int, R)], numPartitions: Int = -1)(implicit rtag: ClassTag[R]): RDD[R] = {
    val _numPartitions = if (numPartitions < 0) rdd.getNumPartitions else numPartitions
    rdd.partitionBy(new KeyPartitioner(_numPartitions)).map(x => x._2)
  }

  /**
    * Returns number of elements on each partition.
    * @param rdd  RDD with objects to process.
    * @tparam R Type of RDD's objects.
    * @return Number of elements on each partition
    */
  def partitionSizes[R](rdd: RDD[R])(implicit rtag: ClassTag[R]): RDD[Int] = {
    rdd.mapPartitions(partitionIt => Iterator(partitionIt.length))
  }
}
