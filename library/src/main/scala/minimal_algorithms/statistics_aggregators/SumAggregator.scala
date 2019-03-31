package minimal_algorithms.statistics_aggregators

class SumAggregator(value: Double) extends StatisticsAggregator {
  def getValue: Double = value

  override def merge(that: StatisticsAggregator): StatisticsAggregator = {
    new SumAggregator(this.value + that.asInstanceOf[SumAggregator].getValue)
  }

  override def toString: String = {
    "SUM: " + this.value.toString
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[SumAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: SumAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}
