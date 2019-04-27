package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.StatisticsMinimalAlgorithmObject
import minimal_algorithms.statistics_aggregators.StatisticsAggregator

abstract class SlidingSMAO(key: Int, weight: Double) extends StatisticsMinimalAlgorithmObject {

  override def toString: String = "Key: " + this.key + " | Weight: " + this.weight

  def getWeight: Double = this.weight

  def getKey: Int = this.key
}
