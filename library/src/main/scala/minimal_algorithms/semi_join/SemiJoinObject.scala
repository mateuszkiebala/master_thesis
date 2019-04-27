package minimal_algorithms.semi_join

import minimal_algorithms.semi_join.SemiJoinSetTypeEnum.SemiJoinSetTypeEnum

trait SemiJoinObject extends Serializable {
  def getSetType: SemiJoinSetTypeEnum
  def getKey: Any
}

object SemiJoinSetTypeEnum extends Enumeration {
  type SemiJoinSetTypeEnum = Value
  val RType, TType = Value
}
