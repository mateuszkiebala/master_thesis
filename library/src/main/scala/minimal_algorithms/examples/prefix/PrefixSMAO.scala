package minimal_algorithms.examples.prefix

import minimal_algorithms.StatisticsMinimalAlgorithmObject
import minimal_algorithms.statistics_aggregators.StatisticsAggregator

abstract class PrefixSMAO[Self <: PrefixSMAO[Self]](weight: Double)
  extends StatisticsMinimalAlgorithmObject[Self] { self: Self =>

  override def compareTo(that: Self): Int = this.weight.compareTo(that.getWeight)

  override def toString: String = "Weight: " + this.weight

  def getWeight: Double = this.weight
}
