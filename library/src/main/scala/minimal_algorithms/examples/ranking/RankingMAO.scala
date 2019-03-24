package minimal_algorithms.examples.ranking

import minimal_algorithms.MinimalAlgorithmObject

class RankingMAO(key: Int, weight: Double) extends MinimalAlgorithmObject[RankingMAO] {
  override def compareTo(o: RankingMAO): Int = {
    val keyCmp = this.key.compareTo(o.getKey)
    if (keyCmp == 0) {
      this.weight.compareTo(o.getWeight)
    } else {
      keyCmp
    }
  }

  override def toString: String = {
    "Key: " + this.key + " | Weight: " + this.weight
  }

  def getWeight: Double = this.weight

  def getKey: Int = this.key

  def canEqual(a: Any): Boolean = a.isInstanceOf[RankingMAO]

  override def equals(that: Any): Boolean =
    that match {
      case that: RankingMAO => that.canEqual(this) && this.key == that.getKey && this.weight == that.getWeight
      case _ => false
    }
}
