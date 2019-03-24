package minimal_algorithms.group_by

import minimal_algorithms.StatisticsMinimalAlgorithmObject
import minimal_algorithms.statistics_aggregators.StatisticsAggregator

class GroupByObject[A <: StatisticsAggregator[A], K <: GroupByKey[K]]
  (aggregator: A, key: K) extends StatisticsMinimalAlgorithmObject[GroupByObject[A, K], A] {

  final override def compareTo(that: GroupByObject[A, K]): Int = {
    this.key.compareTo(that.getKey)
  }

  override def getAggregator: A = this.aggregator

  final def getKey: K = this.key
}
