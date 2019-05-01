package minimal_algorithms.examples.statistics_aggregators

import minimal_algorithms.statistics_aggregators.StatisticsAggregator

class AvgAggregator(sum: Double, count: Int) extends StatisticsAggregator[AvgAggregator] {
  def getSum: Double = sum
  def getCount: Int = count

  override def merge(that: AvgAggregator): AvgAggregator = {
    new AvgAggregator(this.sum + that.getSum, this.count + that.getCount)
  }

  override def toString: String = "%.6f".format(this.getValue)

  def canEqual(a: Any): Boolean = a.isInstanceOf[AvgAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: AvgAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = (this.sum / this.count).hashCode

  def getValue: Double = if (this.count == 0.0) 0.0 else this.sum / this.count
}
