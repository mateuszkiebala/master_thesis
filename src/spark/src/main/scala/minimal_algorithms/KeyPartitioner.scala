package minimal_algorithms.spark
import org.apache.spark.Partitioner

/**
  * Sends an object to partition which index equals object's key.
  * @param numPartitions  Number of partitions.
  */
case class KeyPartitioner(numPartitions: Int) extends Partitioner {
  /**
    * Returns partition index which equals the key.
    * @param key Object key = partition index.
    * @return Index of partition.
    */
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }
}
