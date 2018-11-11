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

// MinimalAlgorithm has to allow for all subclasses of MAO
class MinimalAlgorithm(spark: SparkSession, numOfPartitions: Int) {
  val sc = spark.sparkContext
  var objects: RDD[(Int, MinimalAlgorithmObject)] = sc.emptyRDD
  var balancedObjects: RDD[(Int, MinimalAlgorithmObject)] = sc.emptyRDD
  var itemsCntByPartition: Int = 0
  object PerfectPartitioner {}
  object KeyPartitioner {}

  def importObjects(rdd: RDD[MinimalAlgorithmObject]): this.type = {
    objects = rdd.map{mao => (mao.weight, mao)}
    itemsCntByPartition = (rdd.count().toInt+this.numOfPartitions-1) / this.numOfPartitions
    this
  }

  def teraSort: this.type = {
    import spark.implicits._
    this.objects = this.objects.partitionBy(new RangePartitioner(numOfPartitions, this.objects)).sortByKey().persist()
    this
  }

  def perfectBalance: this.type = {
    val prefixSumsOfPartitionSizes = sc.broadcast(getPartitionSizes(this.objects).collect().scanLeft(0)(_ + _))
    this.balancedObjects = this.objects.mapPartitionsWithIndex((index, partition) => {
        val offset = prefixSumsOfPartitionSizes.value(index)
        if (partition.isEmpty)
          Iterator()
        else
          partition.toList.zipWithIndex.map{
            case (maoPairRDD, i) => (i+offset, maoPairRDD._2)
            case _               => throw new Exception("Error while balancing objects");
          }.toIterator
      })
      .partitionBy(new PerfectPartitioner(numOfPartitions, this.itemsCntByPartition))
      .persist()
    this
  }

  def sendDataToRemotelyRelevantPartitions(windowLen: Int): RDD[(Int, MinimalAlgorithmObject)] = {
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
}
