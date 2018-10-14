import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partitioner, RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD
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
  var sc: SparkContext
  var objects: RDD[MinimalAlgorithmObject]
  var prefixSums: Broadcast[List[Int]]
  var balancedObjects: RDD[(Int, MinimalAlgorithmObject)]
  var itemsCntByPartition: Int
  object PerfectPartitioner {}
  object KeyPartitioner {}

  def MinimalAlgorithm(objects: List[MinimalAlgorithmObject]): Unit = {
    this.sc = spark.sparkContext
    this.objects = sc.parallelize(objects)
    this.itemsCntByPartition = (objects.length+numOfPartitions-1) / numOfPartitions
  }

  // import z pliku - dane kolumnowe
  //def importObjects(objects: List[MinimalAlgorithmObject]): Unit = {
  //  this.objects = sqlContext.sparkContext.parallelize(Seq(objects))
  //}

  // import z pliku - dane w json
  //def importObjects(objects: List[MinimalAlgorithmObject]): Unit = {
  //  this.objects = sqlContext.sparkContext.parallelize(Seq(objects))s
  //}

  def teraSort: this.type = {
    import spark.implicits._
    this.objects = this.objects.partitionBy(new RangePartitioner(this.numOfPartitions, objects.map(o => (o.getWeight(), o)))).persist().sortByKey()
    this
  }

  def perfectBalance: this.type = {
    val prefixSums = sc.broadcast((0 until this.objects.partitions.size)
      .zip(getPartitionsSize().collect().scanLeft(0)(_ + _))
      .toMap)

    this.balancedObjects = this.objects
      .mapPartitions(iter => Iterator(iter.toList.zipWithIndex))
      .mapPartitionsWithIndex((i, iter) => {
        val offset = this.prefixSums.value(i)
        if (iter.isEmpty) Iterator()
        else iter.next.map {case (o, pos) => (pos+offset, o)}.toIterator
      })
      .partitionBy(new PerfectPartitioner(numOfPartitions, itemsCntByPartition))
      .persist()

    this.prefixSums = sc.broadcast(getParitionsWeights(this.balancedObjects)
      .collect().scanLeft(0)(_ + _).toList)

    this
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
  }

  def getPartitionsSize: RDD[Int] = {
    this.objects.mapPartitions(iter => Iterator(iter.length))
  }

  def getParitionsWeights(rdd: RDD[(Int, MinimalAlgorithmObject)]): RDD[Int] = {
    def addWeights(res: Int, x: (Int, MinimalAlgorithmObject)): Int = {
      res + x._2.getWeight()
    }
    rdd.mapPartitions(iter => Iterator(iter.toList.foldLeft(0)(addWeights)))
  }
}
