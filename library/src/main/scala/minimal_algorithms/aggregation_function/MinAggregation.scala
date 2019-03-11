package minimal_algorithms.aggregation_function

class MinAggregation extends AggregationFunction {
  override def defaultValue: Double = Double.MaxValue
  override def apply: (Double, Double) => Double = (x: Double, y: Double) => math.min(x, y)
  override def average: Boolean = false
}
