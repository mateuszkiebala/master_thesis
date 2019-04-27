package minimal_algorithms.examples.ranking

class RankingObject(key: Int, weight: Double) extends Serializable {
  override def toString: String = {
    "Key: " + this.key + " | Weight: " + this.weight
  }

  def getWeight: Double = this.weight

  def getKey: Int = this.key

  def canEqual(a: Any): Boolean = a.isInstanceOf[RankingObject]

  override def equals(that: Any): Boolean =
    that match {
      case that: RankingObject => that.canEqual(this) && this.key == that.getKey && this.weight == that.getWeight
      case _ => false
    }
}

object RankingObject {
  def cmpKey(o: RankingObject): RankingComparator = {
    new RankingComparator(o)
  }
}

class RankingComparator(rank: RankingObject) extends Comparable[RankingComparator] with Serializable {
  override def compareTo(o: RankingComparator): Int = {
    val keyCmp = this.getKey.compareTo(o.getKey)
    if (keyCmp == 0) {
      this.getWeight.compareTo(o.getWeight)
    } else {
      keyCmp
    }
  }

  def getWeight: Double = rank.getWeight
  def getKey: Int = rank.getKey
}
