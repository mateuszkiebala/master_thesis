package minimal_algorithms

import org.apache.spark.Partitioner
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
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

class MinimalAlgorithm(spark: SparkSession, numOfPartitions: Int) {
  var sc = spark.sparkContext
  var objects: RDD[(Int, MinimalAlgorithmObject)] = sc.emptyRDD
  //var prefixSums: Broadcast[List[Int]]
  //var balancedObjects: RDD[(Int, MinimalAlgorithmObject)]
  var itemsCntByPartition: Int = 0
  object PerfectPartitioner {}
  object KeyPartitioner {}

  def importObjects(rdd: RDD[MinimalAlgorithmObject]): this.type = {
    rdd.collect().foreach(println)
    objects = rdd.map{mao => (mao.weight, mao)}
    itemsCntByPartition = (rdd.count().toInt+numOfPartitions-1) / numOfPartitions
    this
  }

  def teraSort: this.type = {
    import spark.implicits._
    objects = objects.partitionBy(new RangePartitioner(numOfPartitions, objects)).persist().sortByKey()
    this
  }


  def perfectBalance: this.type = {
    this.objects.collect().foreach(println)
    //this.prefixSums = sc.broadcast((0 until this.objects.partitions.size)
    //  .zip(getPartitionsSize.collect().scanLeft(0)(_ + _))
    //  .toMap)

    /*this.balancedObjects = this.objects.mapPartitionsWithIndex((i, iter) => {
        val offset = this.prefixSums.value(i)
        if (iter.isEmpty) Iterator()
        else iter.next.map {case (o, pos) => (pos+offset, o)}.toIterator
      })
      .partitionBy(new PerfectPartitioner(numOfPartitions, itemsCntByPartition))
      .persist()

    this.prefixSums = sc.broadcast(getParitionsWeights(this.balancedObjects)
      .collect().scanLeft(0)(_ + _).toList)
     */
    this
  }
  /*
  def sendDataToRemotelyRelevantPartitions(windowLen: Int): RDD[(Int, (Int, MinimalAlgorithmObject))] = {
    this.balancedObjects.mapPartitionsWithIndex((i, iter) => {
      if (windowLen <= this.itemsCntByPartition) {
        iter.map {case (r, mao) =>
          if (i+1 < this.numOfPartitions) {
            List((i, (r, mao)), (i+1, (r, mao)))
          } else {
            List((i, (r, mao)))
          }
        }.flatten.toIterator
      } else {
        val remRelM = (windowLen-1) / this.itemsCntByPartition
        iter.map {case (r, mao) =>
          if (i+remRelM+1 < this.numOfPartitions) {
            List((i, (r, mao)), (i+remRelM, (r, mao)), (i+remRelM+1, (r, mao)))
          } else if (i+remRelM < this.numOfPartitions) {
            List((i, (r, mao)), (i+remRelM, (r, mao)))
          } else {
            List((i, (r, mao)))
          }
        }.flatten.toIterator
      }
    })
  }

  def sendDataToRemotelyRelevantPartitions(rdd: RDD[(Int, (Int, Int))],
    windowLen: Int,
    numOfPartitions: Int,
    itemsCntByPartition: Int): RDD[(Int, (Int, Int, Int))] = {
    rdd.mapPartitionsWithIndex((i, iter) => {
      if (windowLen <= itemsCntByPartition) {
        iter.map {case (r, (k, w)) =>
          if (i+1 < numOfPartitions) {
            List((i, (r, k, w)), (i+1, (r, k, w)))
          } else {
            List((i, (r, k, w)))
          }
        }.flatten.toIterator
      } else {
        val remRelM = (windowLen-1) / itemsCntByPartition
        iter.map {case (r, (k, w)) =>
          if (i+remRelM+1 < numOfPartitions) {
            List((i, (r, k, w)), (i+remRelM, (r, k, w)),
              (i+remRelM+1, (r, k, w)))
          } else if (i+remRelM < numOfPartitions) {
            List((i, (r, k, w)), (i+remRelM, (r, k, w)))
          } else {
            List((i, (r, k, w)))
          }
        }.flatten.toIterator
      }
    })
  }*/

  def getPartitionsSize: RDD[Int] = {
    this.objects.mapPartitions(iter => Iterator(iter.length))
  }

  def getParitionsWeights(rdd: RDD[(Int, MinimalAlgorithmObject)]): RDD[Int] = {
    def addWeights(res: Int, x: (Int, MinimalAlgorithmObject)): Int = {
      res + x._2.weight
    }
    rdd.mapPartitions(iter => Iterator(iter.toList.foldLeft(0)(addWeights)))
  }
}
