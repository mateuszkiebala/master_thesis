package minimal_algorithms.examples

import minimal_algorithms.MinimalAlgorithmObjectWithKey

class SemiJoinType(key: Int, weight: Double, setType: Int) extends MinimalAlgorithmObjectWithKey[SemiJoinType] {
  override def compareTo(that: SemiJoinType): Int = {
    val c = this.key.compareTo(that.getKey)
    if (c != 0) c else this.weight.compareTo(that.getWeight)
  }

  override def getKey: Int = {
    this.key
  }

  override def getWeight: Double = {
    this.weight
  }

  override def toString: String = {
    "Key: " + this.key + " | Set: " + this.setType + " | Weight: " + this.weight
  }

  def getSetType: Int = {
    this.setType
  }
}

object SemiJoinTypeEnum {
  val RType: Int = 0
  val TType: Int = 1
}
