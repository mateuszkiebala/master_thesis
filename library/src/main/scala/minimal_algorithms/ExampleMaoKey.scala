package minimal_algorithms

class ExampleMaoKey(key: Int, weight: Int) extends MinimalAlgorithmObjectWithKey[ExampleMaoKey] {
  override def compareTo(o: ExampleMaoKey): Int = {
    this.key.compareTo(o.getKey)
  }

  override def getKey: Int = {
    this.key
  }

  override def getWeight: Int = {
    this.weight
  }

  override def toString: String = {
    "Key: " + this.key + " | Weight: " + this.weight
  }
}
