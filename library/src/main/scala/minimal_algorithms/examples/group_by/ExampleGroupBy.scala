package minimal_algorithms.group_by
/*
import minimal_algorithms.examples.statistics_aggregators.SumAggregator
import minimal_algorithms.{MinimalAlgorithm, MinimalAlgorithmObject}
import minimal_algorithms.statistics_aggregators._
import org.apache.spark.sql.SparkSession

class InputObject(key: Int, weight: Double) extends Serializable {
  def getKey: Int = this.key
  def getWeight: Double = this.weight
  override def toString: String = key + " " + weight
}

object ExampleGroupBy {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ExampleGroupBy").master("local").getOrCreate()

    val numOfPartitions = args(0).toInt
    val inputPath = args(1)
    val outputPath = args(2)
    val input = spark.sparkContext.textFile(inputPath)

    val inputMapped = input.map(line => {
      val p = line.split(' ')
      new InputObject(p(0).toInt, p(1).toDouble)
    })
    val cmpKey = (o: InputObject) => o.getKey

    val sumAgg = (o: InputObject) => new SumAggregator(o.getWeight)
    val groupedSum = new MinimalGroupBy[InputObject](spark, numOfPartitions).importObjects(inputMapped).groupBy(cmpKey, sumAgg)
    var outputMA = new MinimalAlgorithm[MAOPair](spark, numOfPartitions).importObjects(groupedSum)
    outputMA.perfectSort.map(res => res.getKey.toString + " " + res.getValue.toInt.toString).saveAsTextFile(outputPath + "/output_sum")


    val inputMappedMin = input.map(line => {
      val p = line.split(' ')
      new GroupByObject(new MinAggregator(p(1).toDouble), new IntKey(p(0).toInt))
    })
    val groupedMin = new MinimalGroupBy[GroupByObject](spark, numOfPartitions).importObjects(inputMappedMin).execute.
      map(p => new MAOPair(p._1.asInstanceOf[IntKey].getValue, p._2.asInstanceOf[MinAggregator].getValue))
    outputMA = new MinimalAlgorithm[MAOPair](spark, numOfPartitions).importObjects(groupedMin)
    outputMA.perfectSort.map(res => res.getKey.toString + " " + res.getValue.toInt.toString).saveAsTextFile(outputPath + "/output_min")

    val inputMappedMax = input.map(line => {
      val p = line.split(' ')
      new GroupByObject(new MaxAggregator(p(1).toDouble), new IntKey(p(0).toInt))
    })
    val groupedMax = new MinimalGroupBy[GroupByObject](spark, numOfPartitions).importObjects(inputMappedMax).execute.
      map(p => new MAOPair(p._1.asInstanceOf[IntKey].getValue, p._2.asInstanceOf[MaxAggregator].getValue))
    outputMA = new MinimalAlgorithm[MAOPair](spark, numOfPartitions).importObjects(groupedMax)
    outputMA.perfectSort.map(res => res.getKey.toString + " " + res.getValue.toInt.toString).saveAsTextFile(outputPath + "/output_max")

    val inputMappedAvg = input.map(line => {
      val p = line.split(' ')
      new GroupByObject(new AvgAggregator(p(1).toDouble, 1), new IntKey(p(0).toInt))
    })
    val groupedAvg = new MinimalGroupBy[GroupByObject](spark, numOfPartitions).importObjects(inputMappedAvg).execute.
      map(p => new MAOPair(p._1.asInstanceOf[IntKey].getValue, p._2.asInstanceOf[AvgAggregator].getValue))
    outputMA = new MinimalAlgorithm[MAOPair](spark, numOfPartitions).importObjects(groupedAvg)
    outputMA.perfectSort.map(res => res.getKey.toString + " " + "%.6f".format(res.getValue)).saveAsTextFile(outputPath + "/output_avg")

    spark.stop()
  }
}*/
