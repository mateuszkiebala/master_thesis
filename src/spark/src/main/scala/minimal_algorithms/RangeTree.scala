package minimal_algorithms.spark

import minimal_algorithms.spark.statistics.StatisticsAggregator
import minimal_algorithms.spark.statistics.StatisticsUtils.safeMerge

import scala.reflect.ClassTag

/**
  * Fully binary tree. Enables searching for MAX, MIN, SUM on given range (start, end) in complexity O(n log n)
  * where n is number of initial elements.
  *
  * @param elements list of pairs (value, leaf position into which value will be inserted)
  */
class RangeTree[S <: StatisticsAggregator[S]](elements: Seq[(S, Int)])(implicit stag: ClassTag[S]) extends Serializable {
  val log2: Double => Double = (x: Double) => math.log10(x) / math.log10(2.0)
  val BASE: Int = math.pow(2.0, math.ceil(log2(elements.length.toDouble))).toInt
  var nodes: Array[S] = new Array[S](2 * BASE)
  elements.foreach{ case(element, pos) => insert(element, pos) }

  /**
    * Insert value at given leaf position.
    * @param element  object S
    * @param position [0 ... BASE - 1]
    * @return  Unit
    */
  def insert(element: S, position: Int): Unit = {
    if (position < 0 || position >= BASE)
      throw new IndexOutOfBoundsException("Position out of range: " + position)

    var pos = BASE + position
    nodes(pos) = safeMerge(nodes(pos), element)
    while(pos != 1) {
      pos = pos / 2
      nodes(pos) = safeMerge(nodes(2 * pos), nodes(2 * pos + 1))
    }
  }

  /**
    * Find aggregated value in range. start <= end
    * @param start  [0 ... BASE - 1]
    * @param end  [0 ... BASE - 1]
    * @return  Aggregated value in range.
    */
  def query(start: Int, end: Int): S = {
    if (start > end)
      throw new Exception("Start (" + start + ") greater than end (" + end + ")")
    if (start < 0 || start >= BASE)
      throw new IndexOutOfBoundsException("Position (start) out of range: " + start)
    if (end < 0 || end >= BASE)
      throw new IndexOutOfBoundsException("Position (end) out of range: " + end)

    var vs = BASE + start
    var ve = BASE + end
    var result = nodes(vs)
    if (vs != ve) result = safeMerge(result, nodes(ve))
    while (vs / 2 != ve / 2) {
      if (vs % 2 == 0) result = safeMerge(result, nodes(vs + 1))
      if (ve % 2 == 1) result = safeMerge(result, nodes(ve - 1))
      vs /= 2
      ve /= 2
    }
    result
  }
}
