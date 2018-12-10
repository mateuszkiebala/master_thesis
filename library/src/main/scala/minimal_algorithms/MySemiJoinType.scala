package minimal_algorithms

class MySemiJoinType(key: Int, weight: Int, setType: Int) extends KeyWeightedMAO[MySemiJoinType] {
  override def compareTo(o: MySemiJoinType): Int = {
    val res = this.setType.compareTo(o.getSetType)
    if (res == 0)
      this.key.compareTo(o.getKey)
    else
      res
  }

  override def sortValue: Int = {
    this.getKey
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

object MySemiJoinType {
  val RType: Int = 0
  val TType: Int = 1
}
