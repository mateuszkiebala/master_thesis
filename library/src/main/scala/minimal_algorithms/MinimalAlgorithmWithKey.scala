package minimal_algorithms

import org.apache.spark.sql.SparkSession

class MinimalAlgorithmWithKey[T <: KeyWeightedMAO[T]](spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithm[T](spark, numOfPartitions) {
}
