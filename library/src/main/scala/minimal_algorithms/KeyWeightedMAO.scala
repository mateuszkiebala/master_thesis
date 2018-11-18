package minimal_algorithms

trait KeyWeightedMAO[Self <: KeyWeightedMAO[Self]] extends MinimalAlgorithmObject[Self] { self: Self =>
  def getKey: Int
}
