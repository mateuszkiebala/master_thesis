package minimal_algorithms.semi_join

import minimal_algorithms.MinimalAlgorithmObject

class SemiJoinType(key: Int, weight: Double, setType: Int) extends MinimalAlgorithmObject[SemiJoinType] {
  override def compareTo(that: SemiJoinType): Int = {
    val c = this.key.compareTo(that.getKey)
    if (c != 0) c else this.weight.compareTo(that.getWeight)
  }

  override def toString: String = {
    "Key: " + this.key + " | Set: " + this.setType + " | Weight: " + this.weight
  }

  def getKey: Int = this.key
  def getWeight: Double = this.weight
  def getSetType: Int = this.setType
}

object SemiJoinTypeEnum {
  val RType: Int = 0
  val TType: Int = 1
}
