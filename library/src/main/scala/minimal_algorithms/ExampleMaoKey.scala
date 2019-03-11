package minimal_algorithms

class ExampleMaoKey(key: Int, weight: Double) extends MinimalAlgorithmObjectWithKey[ExampleMaoKey] {
  override def compareTo(o: ExampleMaoKey): Int = {
    this.key.compareTo(o.getKey)
  }

  override def getKey: Int = {
    this.key
  }

  override def getWeight: Double = {
    this.weight
  }

  override def toString: String = {
    "Key: " + this.key + " | Weight: " + this.weight
  }
}
