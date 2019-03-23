package minimal_algorithms.aggregations

trait Aggregator[T] {
  def default: T
  def merge(agg: T): T
}
