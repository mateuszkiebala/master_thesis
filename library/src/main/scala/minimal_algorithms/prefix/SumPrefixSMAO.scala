package minimal_algorithms.prefix

import minimal_algorithms.statistics_aggregators.SumAggregator

class SumPrefixSMAO(weight: Double) extends PrefixSMAO[SumPrefixSMAO, SumAggregator](weight) {
  override def getAggregator: SumAggregator = {
    new SumAggregator(this.weight)
  }
}
