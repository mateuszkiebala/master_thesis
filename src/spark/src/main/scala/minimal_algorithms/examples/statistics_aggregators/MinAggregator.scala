package minimal_algorithms.examples.statistics_aggregators

import minimal_algorithms.statistics.StatisticsAggregator

class MinAggregator(value: Double) extends StatisticsAggregator[MinAggregator] {
  def getValue: Double = value

  override def merge(that: MinAggregator): MinAggregator = {
    new MinAggregator(math.min(this.value, that.getValue))
  }

  override def toString: String = "%.6f".format(this.getValue)

  def canEqual(a: Any): Boolean = a.isInstanceOf[MinAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: MinAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}
