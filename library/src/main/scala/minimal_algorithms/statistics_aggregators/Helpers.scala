package minimal_algorithms.statistics_aggregators

object Helpers {
  def safeMerge[S <: StatisticsAggregator[S]](a: S, b: S): S = {
    if (a == null) {
      b
    } else if (b == null) {
      a
    } else {
      a.merge(b)
    }
  }
}
