package minimal_algorithms

class RangeTree(elements: List[(Int, Int)], aggFun: (Int, Int) => Int, defaultValue: Int) extends Serializable {
  val log2: Double => Double = (x: Double) => math.log10(x) / math.log10(2.0)
  val BASE: Int = math.pow(2.0, math.ceil(log2(elements.length.toDouble))).toInt
  var tree: Array[Int] = (1 to 2 * BASE map(_ => defaultValue)).toArray

  this.elements.foreach{ case(element, pos) => this.insert(element, pos) }

  def insert(value: Int, start: Int) = {
    if (start >= BASE)
      throw new IndexOutOfBoundsException("Position out of range: " + start)

    var pos = BASE + start
    tree(pos) = aggFun(tree(pos), value)
    while(pos != 1) {
      pos = pos / 2
      tree(pos) = aggFun(tree(2 * pos), tree(2 * pos + 1))
    }
  }

  /**
    * Find aggregated value in range. start <= end
    * @param start  [0 ... BASE - 1]
    * @param end  [0 ... BASE - 1]
    * @return  Aggregated value in range.
    */
  def query(start: Int, end: Int) = {
    if (start > end)
      throw new Exception("Start (" + start + ") greater than end (" + end + ")")

    var vs = BASE + start
    var ve = BASE + end
    var result = tree(vs)
    if (vs != ve) result = aggFun(result, tree(ve))
    while (vs / 2 != ve / 2) {
      if (vs % 2 == 0) result = aggFun(result, tree(vs + 1))
      if (ve % 2 == 1) result = aggFun(result, tree(ve - 1))
      vs /= 2
      ve /= 2
    }
    result
  }
}