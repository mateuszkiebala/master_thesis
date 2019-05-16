package minimal_algorithms
import org.apache.spark.Partitioner

/**
  * Distributes data into numerically equal partitions thanks to object's unique key that represents ranking position.
  * @param numPartitions  Number of partitions.
  * @param itemsCntByPartition  Maximum number of items on each partition.
  */
case class PerfectPartitioner(numPartitions: Int, itemsCntByPartition: Int) extends Partitioner {
  /**
    * Returns partition index for given object.
    * @param key Ranking position of the object.
    * @return Partition index (key / itemsCntByPartition).
    */
  override def getPartition(key: Any): Int = {
    key.asInstanceOf[Int] / itemsCntByPartition
  }
}
