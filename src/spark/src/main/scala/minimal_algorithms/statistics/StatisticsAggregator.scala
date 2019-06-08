package minimal_algorithms.statistics

/**
  * Interface responsible for implementing aggregator for statistics
  */
trait StatisticsAggregator[S <: StatisticsAggregator[S]] extends Serializable {
  /**
    * Function providing proper merging of two StatisticsAggregators
    * @param agg  StatisticsAggregator which will be merge into `this`
    * @return   StatisticsAggregator with new merge value
    */
  def merge(agg: S): S
}
