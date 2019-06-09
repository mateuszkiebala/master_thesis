package minimal_algorithms.spark.examples.semi_join

class SemiJoinType(key: Int, weight: Double, setType: Boolean) extends Serializable {
  override def toString: String = this.key + " " + this.weight.toInt
  def getKey: Int = this.key
  def getWeight: Double = this.weight
  def getSetType: Boolean = this.setType
}

object SemiJoinType {
  def cmpKey(o: SemiJoinType): SemiJoinKey = {
    new SemiJoinKey(o)
  }
}

class SemiJoinKey(sjt: SemiJoinType) extends Comparable[SemiJoinKey] with Serializable {
  override def toString: String = this.getKey + " " + this.getWeight

  override def compareTo(o: SemiJoinKey): Int = {
    val keyCmp = this.getKey.compareTo(o.getKey)
    if (keyCmp == 0) {
      this.getWeight.compareTo(o.getWeight)
    } else {
      keyCmp
    }
  }

  override def equals(that: Any): Boolean = {
    that match {
      case that: SemiJoinKey => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  }

  override def hashCode: Int = this.getKey.hashCode

  def canEqual(a: Any): Boolean = a.isInstanceOf[SemiJoinKey]

  def getWeight: Double = sjt.getWeight

  def getKey: Int = sjt.getKey
}
