package minimal_algorithms

/**
  * Interface for more advanced object which can be used in the Minimal Algorithm.
  * We force an object to possess a key and weight.
  * @tparam Self
  */
trait MinimalAlgorithmObjectWithKey[Self <: MinimalAlgorithmObjectWithKey[Self]] extends MinimalAlgorithmObject[Self] { self: Self =>
  /**
    * Provides value that represents a key of the object.
    * Key does not have to be unique.
    * @return Key of the object.
    */
  def getKey: Any
}
