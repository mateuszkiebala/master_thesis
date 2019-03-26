package minimal_algorithms.group_by

import minimal_algorithms.StatisticsMinimalAlgorithmObject
import minimal_algorithms.statistics_aggregators.StatisticsAggregator

class GroupByObject[K <: GroupByKey[K]]
  (aggregator: StatisticsAggregator, key: K) extends StatisticsMinimalAlgorithmObject[GroupByObject[K]] {

  final override def compareTo(that: GroupByObject[K]): Int = {
    this.key.compareTo(that.getKey)
  }

  override def getAggregator: StatisticsAggregator = this.aggregator

  final def getKey: K = this.key
}
