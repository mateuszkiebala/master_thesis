package minimal_algorithms

/**
  * Interface for the most basic object which can be used in Minimal Algorithm.
  * @tparam Self
  */
trait MinimalAlgorithmObject[Self <: MinimalAlgorithmObject[Self]] extends Comparable[Self] with Serializable { self: Self =>
  /**
    * Provides integer value that represents weight of the object.
    * @return Weight of the object.
    */
  def getWeight: Int

  /**
    * Provides integer value that represents weight of the object.
    * @return
    */
  def sortValue: Int
}
