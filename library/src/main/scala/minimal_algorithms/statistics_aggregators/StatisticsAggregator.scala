package minimal_algorithms.statistics_aggregators

trait StatisticsAggregator[T] extends Serializable {
  def merge(agg: T): T
}
