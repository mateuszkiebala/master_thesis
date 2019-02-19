package minimal_algorithms

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

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

class MinimalAlgorithm[T <: MinimalAlgorithmObject[T] : ClassTag](spark: SparkSession, numOfPartitions: Int) {
  protected val sc = spark.sparkContext
  protected var objects: RDD[T] = sc.emptyRDD
  var itemsCntByPartition: Int = 0
  object PerfectPartitioner {}
  object KeyPartitioner {}

  def importObjects(rdd: RDD[T]): this.type = {
    this.objects = rdd
    itemsCntByPartition = (rdd.count().toInt+this.numOfPartitions-1) / this.numOfPartitions
    this
  }

  def teraSort: this.type = {
    this.objects = teraSorted(this.objects)
    this
  }

  /** Super funkcja
    * @param rdd
    * @return
    */
  def teraSorted(rdd: RDD[T]): RDD[T] = {
    import spark.implicits._
    val pairedObjects = rdd.map{mao => (mao, mao)}
    pairedObjects.partitionBy(new RangePartitioner[T, T](numOfPartitions, pairedObjects))
      .sortByKey().values.persist()
  }

  def computePrefixSum: RDD[(Int, T)] = {
    val prefixSumsOfObjects = sc.broadcast(getPartitionSums(this.objects).collect().scanLeft(0)(_ + _))
    this.objects.mapPartitionsWithIndex((index, partition) => {
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

  def computeRanking: RDD[(Int, T)] = computeRanking(this.objects)

  def computeRanking(rdd: RDD[T]): RDD[(Int, T)] = {
    val prefixSumsOfPartitionSizes = sc.broadcast(getPartitionSizes(rdd).collect().scanLeft(0)(_+_))
    rdd.mapPartitionsWithIndex((index, partition) => {
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

  def perfectBalance: this.type = {
    this.objects = perfectlyBalanced(this.objects).map(o => o._2).persist()
    this
  }

  def perfectlyBalanced(rdd: RDD[T]): RDD[(Int, T)] = {
    computeRanking(rdd).partitionBy(new PerfectPartitioner(numOfPartitions, this.itemsCntByPartition)).persist()
  }

  def perfectSort: this.type = {
    teraSort.perfectBalance
    this
  }

  def perfectlySortedWithRanks: RDD[(Int, T)] = {
    perfectlyBalanced(teraSorted(this.objects))
  }

  def distributeDataToRemotelyRelevantPartitions(rdd: RDD[(Int, T)], windowLen: Int): RDD[(Int, T)] = {
    val numOfPartitionsBroadcast = sc.broadcast(this.numOfPartitions).value
    val itemsCntByPartitionBroadcast = sc.broadcast(this.itemsCntByPartition).value
    rdd.mapPartitionsWithIndex((pIndex, partition) => {
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

  private[this] def getPartitionSums(rdd: RDD[T]): RDD[Int] = {
    rdd.mapPartitions(partition => Iterator(partition.map(o => o.getWeight).sum))
  }
}
