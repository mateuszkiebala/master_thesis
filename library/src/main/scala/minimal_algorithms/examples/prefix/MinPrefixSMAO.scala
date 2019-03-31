package minimal_algorithms.examples.prefix

import minimal_algorithms.statistics_aggregators.MinAggregator

class MinPrefixSMAO(weight: Double) extends PrefixSMAO[MinPrefixSMAO](weight) {
  override def getAggregator: MinAggregator = {
    new MinAggregator(this.weight)
  }
}
