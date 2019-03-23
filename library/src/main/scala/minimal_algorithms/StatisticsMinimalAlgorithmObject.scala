package minimal_algorithms

import minimal_algorithms.statistics_aggregators.StatisticsAggregator

trait StatisticsMinimalAlgorithmObject[Self <: StatisticsMinimalAlgorithmObject[Self, A], A <: StatisticsAggregator[A]]
  extends MinimalAlgorithmObject[Self] { self: Self =>

  def getAggregator: A
}
