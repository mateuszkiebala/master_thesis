package minimal_algorithms.aggregations

class SumAggregator(value: Double) extends Aggregator[SumAggregator] {
  def getValue: Double = value

  override def default: SumAggregator = {
    new SumAggregator(0.0)
  }

  override def merge(that: SumAggregator): SumAggregator = {
    new SumAggregator(this.value + that.getValue)
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[SumAggregator]

  override def equals(that: Any): Boolean =
    that match {
      case that: SumAggregator => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode
}

package object SumImplicits {
  implicit val sumAggregator: SumAggregator = new SumAggregator(0.0)
}