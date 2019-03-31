package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.statistics_aggregators.SumAggregator

class SumSlidingSMAO(key: Int, weight: Double) extends SlidingSMAO[SumSlidingSMAO](key, weight) {
  override def getAggregator: SumAggregator = new SumAggregator(weight)
}
