package minimal_algorithms

import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

class MinimalAlgorithmWithKey[T <: MinimalAlgorithmObjectWithKey[T] : ClassTag](spark: SparkSession, numOfPartitions: Int)
  extends MinimalAlgorithm[T](spark, numOfPartitions) {
}
