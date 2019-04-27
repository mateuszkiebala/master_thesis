package minimal_algorithms.group_by

import minimal_algorithms.StatisticsMinimalAlgorithmObject
import minimal_algorithms.statistics_aggregators.StatisticsAggregator

class GroupByObject(aggregator: StatisticsAggregator, key: GroupByKey) extends StatisticsMinimalAlgorithmObject {
  override def getAggregator: StatisticsAggregator = this.aggregator

  final def getKey: GroupByKey = this.key
}

object GroupByObject {
  def cmpKey(o: GroupByObject): GroupByKey = {
    o.getKey
  }
}
