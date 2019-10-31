package minimal_algorithms.factory.examples.spark

import minimal_algorithms.factory.MinimalAlgFactory
import minimal_algorithms.spark.examples.statistics_aggregators.SumAggregator
import org.apache.spark.sql.SparkSession

class InputObject(weight: Double) extends Serializable {
    def getWeight: Double = this.weight
    override def toString: String = "%.6f".format(this.weight)
}

object ExamplePrefix {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("ExamplePrefix").master("local").getOrCreate()
        val numOfPartitions = args(0).toInt
        val inputPath = args(1)
        val outputPath = args(2)

        val input = spark.sparkContext.textFile(inputPath).map(line => {
            val p = line.split(' ')
            new InputObject(p(0).toDouble)
        })

        val cmpKey = (o: InputObject) => o.getWeight
        val sumAgg = (o: InputObject) => new SumAggregator(o.getWeight)
        MinimalAlgFactory.spark(spark, numOfPartitions).prefix(input, cmpKey, sumAgg).saveAsTextFile(outputPath)
        spark.stop()
    }
}
