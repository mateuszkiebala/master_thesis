package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class MinimalGroupBy[T <: KeyWeightedMAO[T]](spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithmWithKey[T](spark, numOfPartitions) {

  def groupBySum: RDD[(Int, Int)] = {
    this.groupBy(0, (x: Int, y: Int) => x + y)
  }

  def groupByMin: RDD[(Int, Int)] = {
    this.groupBy(Int.MaxValue, (x: Int, y: Int) => math.min(x, y))
  }

  def groupByMax: RDD[(Int, Int)] = {
    this.groupBy(0, (x: Int, y: Int) => math.max(x, y))
  }

  private[this] def groupBy(startEle: Int, fun: (Int, Int) => Int): RDD[(Int, Int)] = {
    val masterIndex = 0
    this.objects.asInstanceOf[RDD[KeyWeightedMAO[T]]].mapPartitionsWithIndex((pIndex, partition) => {
      val grouped = partition.toList.groupBy(o => o.getKey)
      val minKey = grouped.keys.min
      val maxKey = grouped.keys.max
      grouped.map{ case (k, v) => {
        (if (k == minKey || k == maxKey) masterIndex else pIndex, (k, v.foldLeft(startEle)((res, o) => fun(res, o.getWeight))))
      }}(collection.breakOut).toIterator
    }).partitionBy(new KeyPartitioner(this.numOfPartitions)).map(p => p._2)
      .mapPartitionsWithIndex((pIndex, partition) => {
        if (pIndex == masterIndex) {
          partition.toList
            .groupBy{ case (k, _) => k }
            .map{ case (k, v) => (k, v.foldLeft(startEle)((res, p) => fun(res, p._2))) }(collection.breakOut)
            .toIterator
        } else {
          partition
        }
      }).persist()
  }
}
