package minimal_algorithms

trait MinimalAlgorithmObject[Self <: MinimalAlgorithmObject[Self]] extends Comparable[Self] with Serializable { self: Self =>
  def getWeight: Int
}
