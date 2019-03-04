package minimal_algorithms.aggregation_function

abstract class AggregationFunction extends Serializable {
  /**
    * @return Default value for given aggregation function. It is a start value for iteration ex. foldLeft(defaultValue){...}
    *         and as well it is used in RangeTree initialization.
    */
  def defaultValue: Int

  /**
    * @return Aggregation function ex. (x: Int, y: Int) => x + y
    */
  def apply: (Int, Int) => Int

  /**
    * @return Flag indicating whether we should use average result or not.
    */
  def average: Boolean
}
