package minimal_algorithms.statistics_aggregators

object Helpers {
  def safeMerge(a: StatisticsAggregator, b: StatisticsAggregator): StatisticsAggregator = {
    if (a == null) {
      b
    } else if (b == null) {
      a
    } else {
      a.merge(b)
    }
  }
}
