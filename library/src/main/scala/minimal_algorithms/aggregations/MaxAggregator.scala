package minimal_algorithms.aggregations

class MaxAggregator(value: Double) extends Aggregator[MaxAggregator] {
  def getValue: Double = value

  override def default: MaxAggregator = {
    new MaxAggregator(Double.MinValue)
  }

  override def merge(that: MaxAggregator): MaxAggregator = {
    new MaxAggregator(math.max(this.value, that.getValue))
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[MaxAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: MaxAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}

package object MaxImplicits {
  implicit val maxAggregator: MaxAggregator = new MaxAggregator(Double.MinValue)
}
