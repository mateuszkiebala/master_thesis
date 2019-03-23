package minimal_algorithms.aggregations

class MinAggregator(value: Double) extends Aggregator[MinAggregator] {
  def getValue: Double = value

  override def default: MinAggregator = {
    new MinAggregator(Double.MaxValue)
  }

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

package object MinImplicits {
  implicit val minAggregator: MinAggregator = new MinAggregator(Double.MaxValue)
}
