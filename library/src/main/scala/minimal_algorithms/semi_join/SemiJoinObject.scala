package minimal_algorithms.semi_join

import minimal_algorithms.semi_join.SemiJoinSetTypeEnum.SemiJoinSetTypeEnum

abstract class SemiJoinObject extends Serializable {
  def getSetType: SemiJoinSetTypeEnum
}

object SemiJoinSetTypeEnum extends Enumeration {
  type SemiJoinSetTypeEnum = Value
  val RType, TType = Value
}
