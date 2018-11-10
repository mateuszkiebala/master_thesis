package minimal_algorithms

class MinimalAlgorithmObject(o: Object, weight: Int) extends Ordered[MinimalAlgorithmObject] {
  def compare(mao: MinimalAlgorithmObject): Int = this.weight
  def getWeight: Int = this.weight
}
