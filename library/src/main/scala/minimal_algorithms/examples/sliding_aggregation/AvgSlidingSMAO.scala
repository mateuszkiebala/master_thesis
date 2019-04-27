package minimal_algorithms.examples.sliding_aggregation

import minimal_algorithms.statistics_aggregators.AvgAggregator

class AvgSlidingSMAO(key: Int, weight: Double) extends SlidingSMAO(key, weight) {
  override def getAggregator: AvgAggregator = new AvgAggregator(weight, 1)
}

object AvgSlidingSMAO {
  def cmpKey(o: AvgSlidingSMAO): AvgSlidingComparator = {
    new AvgSlidingComparator(o)
  }
}

class AvgSlidingComparator(slidingObject: AvgSlidingSMAO) extends Comparable[AvgSlidingComparator] with Serializable {
  override def compareTo(o: AvgSlidingComparator): Int = {
    this.getKey.compareTo(o.getKey)
  }

  def getKey: Int = slidingObject.getKey
}