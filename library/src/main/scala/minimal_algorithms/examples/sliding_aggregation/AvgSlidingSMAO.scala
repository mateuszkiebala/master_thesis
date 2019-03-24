package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.statistics_aggregators.AvgAggregator

class AvgSlidingSMAO(key: Int, weight: Double) extends SlidingSMAO[AvgSlidingSMAO, AvgAggregator](key, weight) {
  override def getAggregator: AvgAggregator = new AvgAggregator(weight, 1)
}
