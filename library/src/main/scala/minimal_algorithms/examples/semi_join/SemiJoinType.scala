package minimal_algorithms.examples.semi_join

import minimal_algorithms.semi_join.SemiJoinObject
import minimal_algorithms.semi_join.SemiJoinSetTypeEnum.SemiJoinSetTypeEnum

class SemiJoinType(key: Int, weight: Double, setType: SemiJoinSetTypeEnum) extends SemiJoinObject {
  override def toString: String = {
    "Key: " + this.key + " | Set: " + this.setType + " | Weight: " + this.weight
  }

  override def getSetType: SemiJoinSetTypeEnum = this.setType

  override def getKey: Int = this.key

  def getWeight: Double = this.weight
}

object SemiJoinType {
  def cmpKey(o: SemiJoinType): SemiJoinComparator = {
    new SemiJoinComparator(o)
  }
}

class SemiJoinComparator(sjt: SemiJoinType) extends Comparable[SemiJoinComparator] with Serializable {
  override def compareTo(o: SemiJoinComparator): Int = {
    val keyCmp = this.getKey.compareTo(o.getKey)
    if (keyCmp == 0) {
      this.getWeight.compareTo(o.getWeight)
    } else {
      keyCmp
    }
  }

  def getWeight: Double = sjt.getWeight
  def getKey: Int = sjt.getKey
}
