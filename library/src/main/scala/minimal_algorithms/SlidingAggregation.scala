package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SlidingAggregation {
  def computeWindowValues(rdd: RDD[(Int, (Int, Int, Int))],
                          itemsCntByPartition: Int, windowLen: Int,
                          partitionsPrefixWeights: List[Int]) = {
    rdd.mapPartitionsWithIndex((i, iter) => {
      val pIndex = i + 1
      val pEleMinRank = i * itemsCntByPartition
      val pEleMaxRank = pIndex * itemsCntByPartition - 1
      val (keys, values) = iter.toList.unzip
      val originalValues = values.filter {case (r, _, _) => r >= pEleMinRank && r <= pEleMaxRank}
      val prefWeights = ((-1) :: values.map {case (rank, _, _) => rank})
        .zip(values.scanLeft(0)((agg, v) => agg + v._3))
        .toMap

      originalValues.map {case (rank, key, weight) => {
        val a = (rank+1-windowLen+itemsCntByPartition) / itemsCntByPartition
        val alpha = if (a >= 0) a else 0
        val (w2, maxRank) = if (pIndex > alpha && alpha > 0) {
          (partitionsPrefixWeights(pIndex-1) - partitionsPrefixWeights(alpha),
            alpha * itemsCntByPartition - 1)
        } else if (alpha == 0) {
          (partitionsPrefixWeights(pIndex-1), -1)
        } else {
          (0, rank)
        }
        val minRank = if ((rank - windowLen + 1) < 0) 0
        else rank - windowLen + 1
        val w1 = prefWeights(maxRank) - prefWeights(minRank-1)
        val w3 = if (alpha == pIndex) 0
        else prefWeights(rank) - prefWeights(pEleMinRank) + originalValues(0)._3
        (key, w1 + w2 + w3)
      }}.toIterator
    })
  }

  def main(args: List[String]): Unit = {
    val spark = SparkSession.builder().appName("SlidingAggregation")
      .master("local").getOrCreate()

    val input = spark.sparkContext.textFile("hdfs://192.168.0.221:9000/user/mati/small_sample/input")
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new MinimalAlgorithmObject(p(0).toInt, p(1).toInt)})

    val minimalAlgorithm = new MinimalAlgorithm(spark, 5)
    minimalAlgorithm.teraSort //.perfectBalance //.sendDataToRemotelyRelevantPartitions(5)

    /*
    val inputPath = args(0)
    val windowLen = args(1).toInt
    val numOfPartitions = args(3).toInt
    val sc = spark.sparkContext
    val input = sc.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      (p(0).toInt, p(1).toInt)})
    val allItemsCnt = input.count().toInt
    val itemsCntByPartition = (allItemsCnt+numOfPartitions-1) / numOfPartitions

    val teraSort = inputMapped
      .partitionBy(new RangePartitioner(numOfPartitions, inputMapped))
      .persist().sortByKey()
    val prefSums = sc.broadcast((0 until teraSort.partitions.size)
      .zip(getPartitionsSize(teraSort).collect().scanLeft(0)(_ + _))
      .toMap)
    val perfectBalanced = teraSort
      .mapPartitions(iter => Iterator(iter.toList.zipWithIndex))
      .mapPartitionsWithIndex((i, iter) => {
        val offset = prefSums.value(i)
        if (iter.isEmpty) Iterator()
        else iter.next.map {case (o, pos) => (pos+offset, o)}.toIterator
      })
      .partitionBy(new PerfectPartitioner(numOfPartitions, itemsCntByPartition))
      .persist()

    val partitionsPrefixWeights = sc.broadcast(getParitionsWeights(perfectBalanced)
      .collect().scanLeft(0)(_ + _).toList)

    teraSort.unpersist()
    prefSums.unpersist()
    val distData = sendDataToRemotelyRelevantPartitions(perfectBalanced,
      windowLen, numOfPartitions, itemsCntByPartition)
      .partitionBy(new KeyPartitioner(numOfPartitions))
      .persist()

    computeWindowValues(distData, itemsCntByPartition,
      windowLen, partitionsPrefixWeights.value)
      .map(res => res._1.toString + " " + res._2.toString)
      .saveAsTextFile(args(2))
      */
    spark.stop()
  }
}
