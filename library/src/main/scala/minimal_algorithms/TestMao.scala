package minimal_algorithms

class TestMao(weight: Int) extends MinimalAlgorithmObject[TestMao] {
  override def compareTo(o: TestMao): Int = {
    this.weight.compareTo(o.getWeight)
  }

  override def toString: String = {
    "Weight: " + this.weight
  }

  override def getWeight: Int = {
    this.weight
  }

  override def sortValue: Int = {
    this.getWeight
  }
}
