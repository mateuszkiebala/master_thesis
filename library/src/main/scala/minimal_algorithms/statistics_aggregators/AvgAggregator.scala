package minimal_algorithms.statistics_aggregators

class AvgAggregator(sum: Double, count: Int) extends StatisticsAggregator {
  def getSum: Double = sum
  def getCount: Int = count

  override def merge(that: StatisticsAggregator): StatisticsAggregator = {
    val o = that.asInstanceOf[AvgAggregator]
    new AvgAggregator(this.sum + o.getSum, this.count + o.getCount)
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[AvgAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: AvgAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = (this.sum / this.count).hashCode

  def getValue: Double = if (this.count == 0.0) 0.0 else this.sum / this.count
}
