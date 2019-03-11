package minimal_algorithms.aggregation_function

class MaxAggregation extends AggregationFunction {
  override def defaultValue: Double = Double.MinValue
  override def apply: (Double, Double) => Double = (x: Double, y: Double) => math.max(x, y)
  override def average: Boolean = false
}
