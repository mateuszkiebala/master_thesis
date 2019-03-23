package minimal_algorithms

class ExampleMaoKey(key: Int, weight: Double) extends MinimalAlgorithmObjectWithKey[ExampleMaoKey] {
  override def compareTo(o: ExampleMaoKey): Int = {
    this.key.compareTo(o.getKey.asInstanceOf[Int])
  }

  override def getKey: Any = {
    this.key.asInstanceOf[Any]
  }

  override def getWeight: Double = {
    this.weight
  }

  override def toString: String = {
    "Key: " + this.key + " | Weight: " + this.weight
  }
}
