package minimal_algorithms.prefix

import minimal_algorithms.StatisticsMinimalAlgorithmObject
import minimal_algorithms.statistics_aggregators.StatisticsAggregator

abstract class PrefixSMAO[Self <: PrefixSMAO[Self, A], A <: StatisticsAggregator[A]](weight: Double)
  extends StatisticsMinimalAlgorithmObject[Self, A] { self: Self =>

  override def compareTo(that: Self): Int = this.weight.compareTo(that.getWeight)

  override def toString: String = "Weight: " + this.weight

  def getWeight: Double = this.weight
}
