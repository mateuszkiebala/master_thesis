package minimal_algorithms.prefix

import minimal_algorithms.statistics_aggregators.MaxAggregator

class MaxPrefixSMAO(weight: Double) extends PrefixSMAO[MaxPrefixSMAO, MaxAggregator](weight) {
  override def getAggregator: MaxAggregator = {
    new MaxAggregator(this.weight)
  }
}
