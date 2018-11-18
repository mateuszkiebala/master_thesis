package minimal_algorithms

class MyW(weight: Int) extends MinimalAlgorithmObject[MyW] {
  override def compareTo(o: MyW): Int = {
    this.weight.compareTo(o.getWeight)
  }

  override def toString: String = {
    "Weight: " + this.weight
  }

  override def getWeight: Int = {
    this.weight
  }
}
