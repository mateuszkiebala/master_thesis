package minimal_algorithms.statistics_aggregators

object Helpers {
  def safeMerge[A <: StatisticsAggregator[A]](a: A, b: A): A = {
    if (a == null) {
      b
    } else if (b == null) {
      a
    } else {
      a.merge(b)
    }
  }
}
