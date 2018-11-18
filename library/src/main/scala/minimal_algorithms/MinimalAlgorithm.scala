package minimal_algorithms

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession

case class PerfectPartitioner(numPartitions: Int, itemsCntByPartition: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int] / itemsCntByPartition
  }
}

case class KeyPartitioner(numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }
}

class MinimalAlgorithm[T <: MinimalAlgorithmObject[T]](spark: SparkSession, numOfPartitions: Int) {
  val sc = spark.sparkContext
  var objects: RDD[(Int, MinimalAlgorithmObject[T])] = sc.emptyRDD
  var balancedObjects: RDD[(Int, MinimalAlgorithmObject[T])] = sc.emptyRDD
  var itemsCntByPartition: Int = 0
  object PerfectPartitioner {}
  object KeyPartitioner {}

  def importObjects(rdd: RDD[T]): this.type = {
    this.objects = rdd.map{mao => (mao.getWeight, mao)}
    itemsCntByPartition = (rdd.count().toInt+this.numOfPartitions-1) / this.numOfPartitions
    this
  }

  def teraSort: this.type = {
    import spark.implicits._
    this.objects = this.objects.partitionBy(new RangePartitioner(numOfPartitions, this.objects)).sortByKey().persist()
    this
  }

  def computePrefixSum: RDD[(Int, MinimalAlgorithmObject[T])] = {
    val prefixSumsOfObjects = sc.broadcast(getPartitionSums(this.objects).collect().scanLeft(0)(_ + _))
    this.objects.mapPartitionsWithIndex((index, partition) => {
      if (partition.isEmpty) {
        Iterator()
      } else {
        val maoObjects = partition.toList
        val prefSums = maoObjects.map(maoPair => maoPair._2.getWeight).scanLeft(prefixSumsOfObjects.value(index))(_+_).tail
        maoObjects.zipWithIndex.map{
          case (maoPairRDD, i) => (prefSums(i), maoPairRDD._2)
          case _               => throw new Exception("Error while creating prefix sums")
        }.toIterator
      }
    })
  }

  def computeRanking: RDD[(Int, MinimalAlgorithmObject[T])] = {
    val prefixSumsOfPartitionSizes = sc.broadcast(getPartitionSizes(this.objects).collect().scanLeft(0)(_+_))
    this.objects.mapPartitionsWithIndex((index, partition) => {
      val offset = prefixSumsOfPartitionSizes.value(index)
      if (partition.isEmpty)
        Iterator()
      else
        partition.toList.zipWithIndex.map{
          case (maoPairRDD, i) => (i+offset, maoPairRDD._2)
          case _               => throw new Exception("Error while creating ranking")
        }.toIterator
    })
  }

  def perfectBalance: this.type = {
    this.balancedObjects = this.computeRanking.partitionBy(new PerfectPartitioner(numOfPartitions, this.itemsCntByPartition)).persist()
    this
  }

  def perfectSort: this.type = {
    this.teraSort.perfectBalance
    this
  }

  def sendDataToRemotelyRelevantPartitions(windowLen: Int): RDD[(Int, MinimalAlgorithmObject[T])] = {
    val numOfPartitionsBroadcast = sc.broadcast(this.numOfPartitions).value
    val itemsCntByPartitionBroadcast = sc.broadcast(this.itemsCntByPartition).value
    this.balancedObjects.mapPartitionsWithIndex((pIndex, partition) => {
      if (windowLen <= itemsCntByPartitionBroadcast) {
        partition.flatMap {rankMaoPair =>
          if (pIndex+1 < numOfPartitionsBroadcast) {
            List((pIndex, rankMaoPair), (pIndex+1, rankMaoPair))
          } else {
            List((pIndex, rankMaoPair))
          }
        }
      } else {
        val remRelM = (windowLen-1) / itemsCntByPartitionBroadcast
        partition.flatMap {rankMaoPair =>
          if (pIndex+remRelM+1 < numOfPartitionsBroadcast) {
            List((pIndex, rankMaoPair), (pIndex+remRelM, rankMaoPair), (pIndex+remRelM+1, rankMaoPair))
          } else if (pIndex+remRelM < numOfPartitionsBroadcast) {
            List((pIndex, rankMaoPair), (pIndex+remRelM, rankMaoPair))
          } else {
            List((pIndex, rankMaoPair))
          }
        }
      }
    }).partitionBy(new KeyPartitioner(this.numOfPartitions)).map(x => x._2).persist()
  }

  def getPartitionSizes[A](rdd: RDD[A]): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.length))
  }

  private[this] def getPartitionSums(rdd: RDD[(Int, MinimalAlgorithmObject[T])]): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.map(o => o._2.getWeight).sum))
  }
}
