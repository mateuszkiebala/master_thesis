package minimal_algorithms

class ExampleMao(weight: Int) extends MinimalAlgorithmObject[ExampleMao] {
  override def compareTo(o: ExampleMao): Int = {
    this.weight.compareTo(o.getWeight)
  }

  override def toString: String = {
    "Weight: " + this.weight
  }

  override def getWeight: Int = {
    this.weight
  }
}
