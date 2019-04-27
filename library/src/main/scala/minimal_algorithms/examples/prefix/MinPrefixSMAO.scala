package minimal_algorithms.examples.prefix

import minimal_algorithms.statistics_aggregators.MinAggregator

class MinPrefixSMAO(weight: Double) extends PrefixSMAO(weight) {
  override def getAggregator: MinAggregator = {
    new MinAggregator(this.weight)
  }
}

object MinPrefixSMAO {
  def cmpKey(o: MinPrefixSMAO): MinPrefixComparator = {
    new MinPrefixComparator(o)
  }
}

class MinPrefixComparator(prefixObject: MinPrefixSMAO) extends Comparable[MinPrefixComparator] with Serializable {
  override def compareTo(o: MinPrefixComparator): Int = {
    this.getWeight.compareTo(o.getWeight)
  }
  def getWeight: Double = prefixObject.getWeight
}