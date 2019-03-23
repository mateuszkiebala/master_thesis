package minimal_algorithms.statistics_aggregators

class MinAggregator(value: Double) extends StatisticsAggregator[MinAggregator] {
  def getValue: Double = value

  override def merge(that: MinAggregator): MinAggregator = {
    new MinAggregator(math.min(this.value, that.getValue))
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[MinAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: MinAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}
