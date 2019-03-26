package minimal_algorithms.statistics_aggregators

class Sum2Aggregator(value: Double) extends SumAggregator(value) {

  override def merge(that: StatisticsAggregator): StatisticsAggregator = {
    new Sum2Aggregator(this.value + that.asInstanceOf[Sum2Aggregator].getValue)
  }

  override def toString: String = {
    "SUM: " + this.value.toString
  }

  override def canEqual(a: Any): Boolean = a.isInstanceOf[Sum2Aggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: Sum2Aggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}
