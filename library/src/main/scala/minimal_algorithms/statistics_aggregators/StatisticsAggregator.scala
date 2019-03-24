package minimal_algorithms.statistics_aggregators

trait StatisticsAggregator[A] extends Serializable {
  def merge(agg: A): A
}
