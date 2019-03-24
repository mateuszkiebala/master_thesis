package minimal_algorithms.examples.group_by

import minimal_algorithms.group_by.GroupByKey

class IntKey(value: Int) extends GroupByKey[IntKey] {
  override def compareTo(that: IntKey): Int = {
    this.value.compareTo(that.getValue)
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: IntKey => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = this.value.hashCode

  def canEqual(a: Any): Boolean = a.isInstanceOf[IntKey]

  def getValue: Int = this.value
}
