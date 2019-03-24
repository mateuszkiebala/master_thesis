package minimal_algorithms.sliding_aggregation

import minimal_algorithms.statistics_aggregators.MaxAggregator

class MaxSlidingSMAO(key: Int, weight: Double) extends SlidingSMAO[MaxSlidingSMAO, MaxAggregator](key, weight) {
  override def getAggregator: MaxAggregator = new MaxAggregator(weight)
}
