package minimal_algorithms.statistics_aggregators

trait StatisticsAggregator extends Serializable {
  def merge(agg: StatisticsAggregator): StatisticsAggregator
}
