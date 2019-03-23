package minimal_algorithms.aggregations

abstract class AggregationFunction extends Serializable {
  /**
    * @return Default value for given aggregation function. It is a start value for iteration ex. foldLeft(defaultValue){...}
    *         and as well it is used in RangeTree initialization.
    */
  def defaultValue: Double

  /**
    * @return Aggregation function ex. (x: Int, y: Int) => x + y
    */
  def apply: (Double, Double) => Double

  /**
    * @return Flag indicating whether we should use average result or not.
    */
  def average: Boolean
}
