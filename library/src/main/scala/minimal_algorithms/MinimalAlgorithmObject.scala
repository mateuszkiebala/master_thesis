package minimal_algorithms

class MinimalAlgorithmObject(val key: Int, val weight: Int) extends Comparable[MinimalAlgorithmObject] with Serializable {
  override def compareTo(mao: MinimalAlgorithmObject): Int = {
    this.weight.compareTo(mao.weight)
  }

  override def toString: String = {
    "Key: " + this.key + " | Weight: " + this.weight
  }
}
