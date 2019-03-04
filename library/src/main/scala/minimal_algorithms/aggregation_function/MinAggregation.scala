package minimal_algorithms.aggregation_function

class MinAggregation extends AggregationFunction {
  override def defaultValue: Int = Int.MaxValue
  override def apply: (Int, Int) => Int = (x: Int, y: Int) => math.min(x, y)
  override def average: Boolean = false
}
