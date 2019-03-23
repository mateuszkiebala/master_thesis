package minimal_algorithms.prefix

import minimal_algorithms.statistics_aggregators.MinAggregator

class MinPrefixSMAO(weight: Double) extends PrefixSMAO[MinPrefixSMAO, MinAggregator](weight) {
  override def getAggregator: MinAggregator = {
    new MinAggregator(this.weight)
  }
}
