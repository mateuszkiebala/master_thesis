package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.StatisticsMinimalAlgorithmObject
import minimal_algorithms.statistics_aggregators.StatisticsAggregator

abstract class SlidingSMAO[Self <: SlidingSMAO[Self, A], A <: StatisticsAggregator[A]]
  (key: Int, weight: Double) extends StatisticsMinimalAlgorithmObject[Self, A] { self: Self =>

  override def compareTo(that: Self): Int = this.key.compareTo(that.getKey)

  override def toString: String = "Key: " + this.key + " | Weight: " + this.weight

  def getWeight: Double = this.weight

  def getKey: Int = this.key
}
