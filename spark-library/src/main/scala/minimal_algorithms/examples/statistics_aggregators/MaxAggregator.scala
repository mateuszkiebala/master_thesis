package minimal_algorithms.examples.statistics_aggregators

import minimal_algorithms.statistics.StatisticsAggregator

class MaxAggregator(value: Double) extends StatisticsAggregator[MaxAggregator] {
  def getValue: Double = value

  override def merge(that: MaxAggregator): MaxAggregator = {
    new MaxAggregator(math.max(this.value, that.getValue))
  }

  override def toString: String = "%.6f".format(this.getValue)

  def canEqual(a: Any): Boolean = a.isInstanceOf[MaxAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: MaxAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}
