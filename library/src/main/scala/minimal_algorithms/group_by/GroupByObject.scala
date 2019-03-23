package minimal_algorithms.group_by

import minimal_algorithms.MinimalAlgorithmObject

import scala.reflect.ClassTag

abstract class GroupByObject[Self <: GroupByObject[Self, K], K <: GroupByKey[K] : ClassTag](key: K)
  extends MinimalAlgorithmObject[Self] {self: Self =>

  final override def compareTo(that: Self): Int = {
    this.key.compareTo(that.getKey)
  }

  def getKey: K = key
}
