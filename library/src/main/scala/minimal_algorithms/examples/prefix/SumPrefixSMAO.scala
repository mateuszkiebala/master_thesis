package minimal_algorithms.examples.prefix

import minimal_algorithms.statistics_aggregators.SumAggregator

class SumPrefixSMAO(weight: Double) extends PrefixSMAO[SumPrefixSMAO](weight) {
  override def getAggregator: SumAggregator = {
    new SumAggregator(this.weight)
  }
}
