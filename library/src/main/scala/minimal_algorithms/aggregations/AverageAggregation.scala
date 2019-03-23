package minimal_algorithms.aggregations

class AverageAggregation extends AggregationFunction {
  override def defaultValue: Double = 0
  override def apply: (Double, Double) => Double = (x: Double, y: Double) => x + y
  override def average: Boolean = true
}
