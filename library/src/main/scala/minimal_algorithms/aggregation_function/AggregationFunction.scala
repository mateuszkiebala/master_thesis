package minimal_algorithms.aggregation_function

abstract class AggregationFunction extends Serializable {
  def defaultValue: Int
  def apply: (Int, Int) => Int
  def average: Boolean
}
