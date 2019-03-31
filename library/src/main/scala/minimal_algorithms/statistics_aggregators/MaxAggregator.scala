package minimal_algorithms.statistics_aggregators

class MaxAggregator(value: Double) extends StatisticsAggregator {
  def getValue: Double = value

  override def merge(that: StatisticsAggregator): StatisticsAggregator = {
    new MaxAggregator(math.max(this.value, that.asInstanceOf[MaxAggregator].getValue))
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[MaxAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: MaxAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}
