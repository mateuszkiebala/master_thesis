package minimal_algorithms.group_by

trait GroupByKey[Self <: GroupByKey[Self]] extends Comparable[Self] { self: Self =>
  def value: Any
}
