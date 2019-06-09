package minimal_algorithms.spark.examples.statistics_aggregators

import minimal_algorithms.spark.statistics.StatisticsAggregator

class SumAggregator(value: Double) extends StatisticsAggregator[SumAggregator] {
  def getValue: Double = value

  override def merge(that: SumAggregator): SumAggregator = {
    new SumAggregator(this.value + that.getValue)
  }

  override def toString: String = "%.6f".format(this.getValue)

  def canEqual(a: Any): Boolean = a.isInstanceOf[SumAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: SumAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}
