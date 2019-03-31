package minimal_algorithms

import minimal_algorithms.statistics_aggregators.StatisticsAggregator

trait StatisticsMinimalAlgorithmObject[Self <: StatisticsMinimalAlgorithmObject[Self]]
  extends MinimalAlgorithmObject[Self] { self: Self =>

  def getAggregator: StatisticsAggregator
}
