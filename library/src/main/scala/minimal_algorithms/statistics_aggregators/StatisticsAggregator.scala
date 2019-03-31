package minimal_algorithms.statistics_aggregators

/**
  * Interface responsible for implementing aggregator for statistics
  */
trait StatisticsAggregator extends Serializable {
  /**
    * Function profiving proper merging of two StatisticsAggregators
    * @param agg  StatisticsAggregator which will be merge into `this`
    * @return   StatisticsAggregator with new merge value
    */
  def merge(agg: StatisticsAggregator): StatisticsAggregator
}
