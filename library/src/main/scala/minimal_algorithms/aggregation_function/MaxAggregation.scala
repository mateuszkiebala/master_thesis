package minimal_algorithms.aggregation_function

class MaxAggregation extends AggregationFunction {
  override def defaultValue: Int = Int.MinValue
  override def apply: (Int, Int) => Int = (x: Int, y: Int) => math.max(x, y)
  override def average: Boolean = false
}
