package minimal_algorithms.examples

import minimal_algorithms.MinimalAlgorithmObjectWithKey

class SemiJoinType(key: Int, weight: Int, setType: Int) extends MinimalAlgorithmObjectWithKey[SemiJoinType] {
  override def compareTo(o: SemiJoinType): Int = {
    val res = this.setType.compareTo(o.getSetType)
    if (res == 0)
      this.key.compareTo(o.getKey)
    else
      res
  }

  override def getKey: Int = {
    this.key
  }

  override def getWeight: Int = {
    this.weight
  }

  override def toString: String = {
    "Key: " + this.key + " | Set: " + this.setType + " | Weight: " + this.weight
  }

  def getSetType: Int = {
    this.setType
  }
}

object SemiJoinType {
  val RType: Int = 0
  val TType: Int = 1
}
