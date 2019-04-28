package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.statistics_aggregators.MaxAggregator

class MaxSlidingSMAO(key: Int, weight: Double) extends SlidingSMAO(key, weight) {
  override def getAggregator: MaxAggregator = new MaxAggregator(weight)
}

object MaxSlidingSMAO {
  def cmpKey(o: MaxSlidingSMAO): MaxSlidingComparator = {
    new MaxSlidingComparator(o)
  }

  def statsAgg(o: MaxSlidingSMAO): MaxAggregator = {
    new MaxAggregator(o.getWeight)
  }
}

class MaxSlidingComparator(slidingObject: MaxSlidingSMAO) extends Comparable[MaxSlidingComparator] with Serializable {
  override def compareTo(o: MaxSlidingComparator): Int = {
    this.getKey.compareTo(o.getKey)
  }

  def getKey: Int = slidingObject.getKey
}