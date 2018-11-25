package minimal_algorithms

class MyKW(key: Int, weight: Int) extends KeyWeightedMAO[MyKW] {
  override def compareTo(o: MyKW): Int = {
    this.weight.compareTo(o.getWeight)
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
    "Key: " + this.key + " | Weight: " + this.weight
  }
}
