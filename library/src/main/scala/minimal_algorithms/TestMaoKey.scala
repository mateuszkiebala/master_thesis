package minimal_algorithms

class TestMaoKey(key: Int, weight: Int) extends MinimalAlgorithmObjectWithKey[TestMaoKey] {
  override def compareTo(o: TestMaoKey): Int = {
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
