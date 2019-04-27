package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.statistics_aggregators.MinAggregator

class MinSlidingSMAO(key: Int, weight: Double) extends SlidingSMAO(key, weight) {
  override def getAggregator: MinAggregator = new MinAggregator(weight)
}

object MinSlidingSMAO {
  def cmpKey(o: MinSlidingSMAO): MinSlidingComparator = {
    new MinSlidingComparator(o)
  }
}

class MinSlidingComparator(slidingObject: MinSlidingSMAO) extends Comparable[MinSlidingComparator] with Serializable {
  override def compareTo(o: MinSlidingComparator): Int = {
    this.getKey.compareTo(o.getKey)
  }

  def getKey: Int = slidingObject.getKey
}
