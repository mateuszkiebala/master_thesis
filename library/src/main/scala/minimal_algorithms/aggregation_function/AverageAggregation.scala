package minimal_algorithms.aggregation_function

class AverageAggregation extends AggregationFunction {
  override def defaultValue: Int = 0
  override def apply: (Int, Int) => Int = (x: Int, y: Int) => x + y
  override def average: Boolean = true
}
