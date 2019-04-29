package minimal_algorithms.examples.semi_join

import minimal_algorithms.semi_join.SemiJoinObject
import minimal_algorithms.semi_join.SemiJoinSetTypeEnum.SemiJoinSetTypeEnum

class SemiJoinType(key: Int, weight: Double, setType: SemiJoinSetTypeEnum) extends SemiJoinObject {
  override def toString: String = "Key: " + this.key + " | Set: " + this.setType + " | Weight: " + this.weight

  override def getSetType: SemiJoinSetTypeEnum = this.setType

  def getKey: Int = this.key

  def getWeight: Double = this.weight
}

object SemiJoinType {
  def cmpKey(o: SemiJoinType): SemiJoinKey = {
    new SemiJoinKey(o)
  }
}

class SemiJoinKey(sjt: SemiJoinType) extends Comparable[SemiJoinKey] with Serializable {
  override def toString: String = "Key: " + this.getKey + " | Weight: " + this.getWeight

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
