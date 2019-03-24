package minimal_algorithms.semi_join

import minimal_algorithms.MinimalAlgorithmObject
import minimal_algorithms.semi_join.SemiJoinSetTypeEnum.SemiJoinSetTypeEnum

abstract class SemiJoinObject[Self <: SemiJoinObject[Self]] extends MinimalAlgorithmObject[Self] {
  def getSetType: SemiJoinSetTypeEnum
}

object SemiJoinSetTypeEnum extends Enumeration {
  type SemiJoinSetTypeEnum = Value
  val RType, TType = Value
}
