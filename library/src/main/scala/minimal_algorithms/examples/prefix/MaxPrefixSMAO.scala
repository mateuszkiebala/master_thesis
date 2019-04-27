package minimal_algorithms.examples.prefix

import minimal_algorithms.statistics_aggregators.MaxAggregator

class MaxPrefixSMAO(weight: Double) extends PrefixSMAO(weight) {
  override def getAggregator: MaxAggregator = {
    new MaxAggregator(this.weight)
  }
}

object MaxPrefixSMAO {
  def cmpKey(o: MaxPrefixSMAO): MaxPrefixComparator = {
    new MaxPrefixComparator(o)
  }
}

class MaxPrefixComparator(prefixObject: MaxPrefixSMAO) extends Comparable[MaxPrefixComparator] with Serializable {
  override def compareTo(o: MaxPrefixComparator): Int = {
    this.getWeight.compareTo(o.getWeight)
  }
  def getWeight: Double = prefixObject.getWeight
}
