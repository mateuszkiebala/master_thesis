package minimal_algorithms.examples.prefix

import minimal_algorithms.statistics_aggregators.SumAggregator

class SumPrefixSMAO(weight: Double) extends PrefixSMAO(weight) {
  override def getAggregator: SumAggregator = {
    new SumAggregator(this.weight)
  }
}

object SumPrefixSMAO {
  def cmpKey(o: SumPrefixSMAO): SumPrefixComparator = {
    new SumPrefixComparator(o)
  }

  def statsAgg(o: SumPrefixSMAO): SumAggregator = {
    new SumAggregator(o.getWeight)
  }
}

class SumPrefixComparator(prefixObject: SumPrefixSMAO) extends Comparable[SumPrefixComparator] with Serializable {
  override def compareTo(o: SumPrefixComparator): Int = {
    this.getWeight.compareTo(o.getWeight)
  }
  def getWeight: Double = prefixObject.getWeight
}
