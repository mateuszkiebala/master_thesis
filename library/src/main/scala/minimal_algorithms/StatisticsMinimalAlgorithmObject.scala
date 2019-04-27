package minimal_algorithms

import minimal_algorithms.statistics_aggregators.StatisticsAggregator

trait StatisticsMinimalAlgorithmObject extends Serializable {
  def getAggregator: StatisticsAggregator
}
