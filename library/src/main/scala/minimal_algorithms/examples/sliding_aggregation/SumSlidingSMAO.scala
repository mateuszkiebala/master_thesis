package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.statistics_aggregators.SumAggregator

class SumSlidingSMAO(key: Int, weight: Double) extends SlidingSMAO(key, weight) {
  override def getAggregator: SumAggregator = new SumAggregator(weight)
}

object SumSlidingSMAO {
  def cmpKey(o: SumSlidingSMAO): SumSlidingComparator = {
    new SumSlidingComparator(o)
  }
}

class SumSlidingComparator(slidingObject: SumSlidingSMAO) extends Comparable[SumSlidingComparator] with Serializable {
  override def compareTo(o: SumSlidingComparator): Int = {
    this.getKey.compareTo(o.getKey)
  }

  def getKey: Int = slidingObject.getKey
}