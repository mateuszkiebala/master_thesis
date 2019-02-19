package minimal_algorithms

trait MinimalAlgorithmObjectWithKey[Self <: MinimalAlgorithmObjectWithKey[Self]] extends MinimalAlgorithmObject[Self] { self: Self =>
  def getKey: Int
}
