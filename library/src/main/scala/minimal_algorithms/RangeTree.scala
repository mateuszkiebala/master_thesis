package minimal_algorithms

import minimal_algorithms.aggregations.Aggregator

import scala.reflect.ClassTag

/**
  * Fully binary tree. Enables searching for MAX, MIN, SUM on given range (start, end) in complexity O(n log n)
  * where n is number of initial elements.
  *
  * @param elements list of pairs (value, leaf position into which value will be insertedą)
  * @param aggFun aggregation function that will be applied on the tree (MAX, MIN, SUM)
  */
class RangeTree[A <: Aggregator[A] : ClassTag](elements: Array[(A, Int)])(implicit aggType: Aggregator[A]) extends Serializable {
  val log2: Double => Double = (x: Double) => math.log10(x) / math.log10(2.0)
  val BASE: Int = math.pow(2.0, math.ceil(log2(elements.length.toDouble))).toInt
  var tree: Array[A] = (1 to 2 * BASE map(_ => aggType.default)).toArray

  this.elements.foreach{ case(element, pos) => this.insert(element, pos) }

  private[this] def insert(agg: A, start: Int): Unit = {
    if (start >= BASE)
      throw new IndexOutOfBoundsException("Position out of range: " + start)

    var pos = BASE + start
    tree(pos) = tree(pos).merge(agg)
    while(pos != 1) {
      pos = pos / 2
      tree(pos) = tree(2 * pos).merge(tree(2 * pos + 1))
    }
  }

  /**
    * Find aggregated value in range. start <= end
    * @param start  [0 ... BASE - 1]
    * @param end  [0 ... BASE - 1]
    * @return  Aggregated value in range.
    */
  def query(start: Int, end: Int): A = {
    if (start > end)
      throw new Exception("Start (" + start + ") greater than end (" + end + ")")

    var vs = BASE + start
    var ve = BASE + end
    var result = tree(vs)
    if (vs != ve) result = result.merge(tree(ve))
    while (vs / 2 != ve / 2) {
      if (vs % 2 == 0) result = result.merge(tree(vs + 1))
      if (ve % 2 == 1) result = result.merge(tree(ve - 1))
      vs /= 2
      ve /= 2
    }
    result
  }
}
