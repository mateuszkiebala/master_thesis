package minimal_algorithms

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Utils {
  object KeyPartitioner {}

  /**
    * Shuffles rdd objects by sending them to given machine indices.
    * Method does NOT preserve order.
    * @param rdd  RDD of pairs (object, List[destination machine index])
    * @tparam T [T : ClassTag]
    * @return RDD of reshuffled objects
    */
  def sendToMachines[T](rdd: RDD[(T, Seq[Int])])(implicit rtag: ClassTag[T]): RDD[T] = {
    partitionByKey[T](rdd.mapPartitionsWithIndex((pIndex, partition) => {
      partition.flatMap{case (o, indices) => if (indices.isEmpty) List((pIndex, o)) else indices.map{i => (i, o)}}
    }))
  }

  /**
    * Creates RDD (with numPartitions partitions) from arr.
    * Then shuffles rdd objects by sending them to machines [machineIndexLowerBound, numPartitions)
    * Method does NOT preserve order.
    * @param arr  Seq[(object, machineIndexLowerBound)]
    */
  def sendToAllHigherMachines[T](sc: SparkContext, arr: Seq[(T, Int)], numPartitions: Int)(implicit rtag: ClassTag[T]): RDD[T] = {
    sendToAllHigherMachines(sc.parallelize(arr, numPartitions))
  }

  /**
    * Shuffles rdd objects by sending them to machines [machineIndexLowerBound, numPartitions)
    * Method does NOT preserve order.
    * @param rdd  RDD[(object, machineIndexLowerBound)]
    */
  def sendToAllHigherMachines[T](rdd: RDD[(T, Int)])(implicit rtag: ClassTag[T]): RDD[T] = {
    val numPartitions = rdd.getNumPartitions
    partitionByKey(rdd.mapPartitions(partition => {
      partition.flatMap{case (o, lowerBound) => List.range(lowerBound, numPartitions).map{i => (i, o)}}
    }))
  }

  /**
    * Creates RDD (with numPartitions partitions) from arr.
    * Then shuffles rdd objects by sending them to machines [0, min(machineIndexUpperBound, numPartitions))
    * Method does NOT preserve order.
    * @param arr  Seq[(object, machineIndexUpperBound)]
    */
  def sendToAllLowerMachines[T](sc: SparkContext, arr: Seq[(T, Int)], numPartitions: Int)(implicit rtag: ClassTag[T]): RDD[T] = {
    sendToAllLowerMachines(sc.parallelize(arr, numPartitions))
  }

  /**
    * Shuffles rdd objects by sending them to machines [0, min(machineIndexUpperBound, numPartitions))
    * Method does NOT preserve order.
    * @param rdd  RDD[(object, machineIndexUpperBound)]
    */
  def sendToAllLowerMachines[T](rdd: RDD[(T, Int)])(implicit rtag: ClassTag[T]): RDD[T] = {
    partitionByKey[T](rdd.mapPartitions(partition => {
      partition.flatMap{case (o, upperBound) => List.range(0, upperBound).map{i => (i, o)}}
    }))
  }

  /**
    * Creates RDD (with numPartitions partitions) from arr.
    * Then shuffles rdd objects by sending them to machines [machineIndexLowerBound, min(machineIndexUpperBound, numPartitions))
    * Method does NOT preserve order.
    * @param arr  Seq[(object, machineIndexLowerBound, machineIndexUpperBound)]
    */
  def sendToRangeMachines[T](sc: SparkContext, arr: Seq[(T, Int, Int)], numPartitions: Int)(implicit rtag: ClassTag[T]): RDD[T] = {
    sendToRangeMachines(sc.parallelize(arr, numPartitions))
  }

  /**
    * Shuffles rdd objects by sending them to machines [machineIndexLowerBound, min(machineIndexUpperBound, numPartitions))
    * Method does NOT preserve order.
    * @param rdd  RDD[(object, machineIndexLowerBound, machineIndexUpperBound)]
    */
  def sendToRangeMachines[T](rdd: RDD[(T, Int, Int)])(implicit rtag: ClassTag[T]): RDD[T] = {
    partitionByKey[T](rdd.mapPartitions(partition => {
      partition.flatMap{case (o, lowerBound, upperBound) => List.range(lowerBound, upperBound).map{i => (i, o)}}
    }))
  }

  /**
    * Broadcasts object to all partitions.
    */
  def sendToAllMachines[T](sc: SparkContext, o: T)(implicit rtag: ClassTag[T]): T = {
    sc.broadcast(o).value
  }

  def partitionByKey[T](rdd: RDD[(Int, T)], numPartitions: Int = -1)(implicit rtag: ClassTag[T]): RDD[T] = {
    val _numPartitions = if (numPartitions < 0) rdd.getNumPartitions else numPartitions
    rdd.partitionBy(new KeyPartitioner(_numPartitions)).map(x => x._2)
  }

  /**
    * Returns number of elements on each partition.
    * @param rdd  RDD with objects to process.
    * @tparam T Type of RDD's objects.
    * @return Number of elements on each partition
    */
  def partitionSizes[T](rdd: RDD[T])(implicit rtag: ClassTag[T]): RDD[Int] = {
    rdd.mapPartitions(partitionIt => Iterator(partitionIt.length))
  }
}
