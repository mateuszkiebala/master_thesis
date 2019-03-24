package minimal_algorithms.sliding_aggregation

import minimal_algorithms.statistics_aggregators.MinAggregator

class MinSlidingSMAO(key: Int, weight: Double) extends SlidingSMAO[MinSlidingSMAO, MinAggregator](key, weight) {
  override def getAggregator: MinAggregator = new MinAggregator(weight)
}
