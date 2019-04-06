package minimal_algorithms.examples.semi_join

import minimal_algorithms.semi_join.SemiJoinObject
import minimal_algorithms.semi_join.SemiJoinSetTypeEnum.SemiJoinSetTypeEnum

class SemiJoinType(key: Int, weight: Double, setType: SemiJoinSetTypeEnum) extends SemiJoinObject[SemiJoinType] {
  override def compareTo(that: SemiJoinType): Int = {
    val c = this.key.compareTo(that.getKey)
    if (c != 0) c else this.weight.compareTo(that.getWeight)
  }

  override def toString: String = {
    "Key: " + this.key + " | Set: " + this.setType + " | Weight: " + this.weight
  }

  override def getSetType: SemiJoinSetTypeEnum = this.setType

  override def getKey: Int = this.key

  def getWeight: Double = this.weight
}