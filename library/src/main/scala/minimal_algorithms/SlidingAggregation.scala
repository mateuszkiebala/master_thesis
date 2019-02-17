package minimal_algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SlidingAggregation {
  /*def getPartitionsWeights(rdd: RDD[MyKW]): RDD[Int] = {
    def addWeights(res: Int, o: MyKW): Int = {
      res + o.getWeight
    }
    rdd.mapPartitions(partition => Iterator(partition.toList.foldLeft(0)(addWeights)))
  }

  def computeWindowValues(rdd: RDD[(Int, MyKW)],
                          itemsCntByPartition: Int, windowLen: Int,
                          partitionsPrefixWeights: List[Int]): RDD[(Int, Int)] = {
    rdd.mapPartitionsWithIndex((index, partition) => {
      val pIndex = index + 1
      val pEleMinRank = index * itemsCntByPartition
      val pEleMaxRank = pIndex * itemsCntByPartition - 1
      val partitionObjects = partition.toList.sorted
      val baseObjects = partitionObjects.filter {case (rank, _) => rank >= pEleMinRank && rank <= pEleMaxRank}
      val prefixWeights = ((-1) :: partitionObjects.map {case (rank, _) => rank})
        .zip(partitionObjects.scanLeft(0)((result, rankMaoPair) => result + rankMaoPair._2.getWeight)).toMap

      baseObjects.map {case (rank, mao) => {
        val a = (rank+1-windowLen+itemsCntByPartition) / itemsCntByPartition
        val alpha = if (a >= 0) a else 0
        val (w2, maxRank) = if (pIndex > alpha && alpha > 0) {
          (partitionsPrefixWeights(pIndex-1)-partitionsPrefixWeights(alpha), alpha*itemsCntByPartition-1)
        } else if (alpha == 0) {
          (partitionsPrefixWeights(pIndex-1), -1)
        } else {
          (0, rank)
        }
        val minRank = if ((rank - windowLen + 1) < 0) 0 else rank - windowLen + 1
        val w1 = prefixWeights(maxRank) - prefixWeights(minRank-1)
        val w3 = if (alpha == pIndex) 0 else prefixWeights(rank) - prefixWeights(pEleMinRank) + baseObjects.head._2.getWeight
        (mao.getKey, w1 + w2 + w3)
      }}.toIterator
    })
  }*/

  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("SlidingAggregation").master("local").getOrCreate()

    //val inputPath = "hdfs://192.168.0.220:9000/user/mati/test.txt"
    val windowLen = 4
    val inputPath = "test.txt"
    val outputPath = "out_sliding_agg"
    val input = spark.sparkContext.textFile(inputPath)
    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new MyKW(p(0).toInt, p(1).toInt)})

    //val minimalAlgorithm = new MinimalAlgorithmWithKey[MyKW](spark, 5)
    //val distData = minimalAlgorithm.importObjects(inputMapped).perfectSort.distributeData(windowLen)
    //val prefixedWeights = spark.sparkContext.broadcast(getPartitionsWeights(minimalAlgorithm.getObjects).collect().scanLeft(0)(_ + _).toList).value
    //computeWindowValues(distData, minimalAlgorithm.itemsCntByPartition, windowLen, prefixedWeights)
    // .map(res => res._1.toString + " " + res._2.toString).saveAsTextFile(outputPath)

    val msa = new MinimalSlidingAggregation[MyKW](spark, 5)
    msa.execute(inputMapped, windowLen)
    spark.stop()
  }
}
