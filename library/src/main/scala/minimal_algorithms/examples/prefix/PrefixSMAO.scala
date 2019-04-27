package minimal_algorithms.examples.prefix

import minimal_algorithms.StatisticsMinimalAlgorithmObject
import minimal_algorithms.statistics_aggregators.StatisticsAggregator

abstract class PrefixSMAO(weight: Double) extends StatisticsMinimalAlgorithmObject {

  override def toString: String = "Weight: " + this.weight

  def getWeight: Double = this.weight
}
