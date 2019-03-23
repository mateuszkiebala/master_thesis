package minimal_algorithms

/**
  * Interface for the most basic object which can be used in the Minimal Algorithm.
  * @tparam Self
  */
trait MinimalAlgorithmObject[Self <: MinimalAlgorithmObject[Self]] extends Comparable[Self] with Serializable {}
