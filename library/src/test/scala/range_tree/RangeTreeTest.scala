package range_tree
import org.scalatest.FunSuite
import minimal_algorithms.RangeTree
import minimal_algorithms.aggregations._
import minimal_algorithms.aggregations.SumImplicits._
import minimal_algorithms.aggregations.MinImplicits._
import minimal_algorithms.aggregations.MaxImplicits._

class RangeTreeTest extends FunSuite {
  def sumWrapResult(elements: Array[Double]): Array[SumAggregator] = {
    elements.map{e => new SumAggregator(e)}
  }

  def sumWrapInsert(elements: Array[(Double, Int)]): Array[(SumAggregator, Int)] = {
    elements.map{case(e, pos) => (new SumAggregator(e), pos)}
  }

  def minWrapResult(elements: Array[Double]): Array[MinAggregator] = {
    elements.map{e => new MinAggregator(e)}
  }

  def minWrapInsert(elements: Array[(Double, Int)]): Array[(MinAggregator, Int)] = {
    elements.map{case(e, pos) => (new MinAggregator(e), pos)}
  }

  def maxWrapResult(elements: Array[Double]): Array[MaxAggregator] = {
    elements.map{e => new MaxAggregator(e)}
  }

  def maxWrapInsert(elements: Array[(Double, Int)]): Array[(MaxAggregator, Int)] = {
    elements.map{case(e, pos) => (new MaxAggregator(e), pos)}
  }

  test("RangeTree.init one node tree") {
      // given
    val elements = sumWrapInsert(Array((1.0, 0)))

      // when
    val rangeTree = new RangeTree[SumAggregator](elements)

      // then
    assert(1 == rangeTree.BASE)
    assert(sumWrapResult(Array(0.0, 1.0)).sameElements(rangeTree.tree))
  }

  test("RangeTree.init position out of range") {
      // given
    val elements = sumWrapInsert(Array((1.0, 0), (2.0, 4), (3.0, 2), (4.0, 3)))

      // when, then
    val caught = intercept[Exception] {
        new RangeTree[SumAggregator](elements)
      }

    assert(caught.getMessage == "Position out of range: 4")
  }

  test("RangeTree.init start > end") {
      // given
    val elements = sumWrapInsert(Array((1.0, 0), (2.0, 1), (3.0, 2), (3.0, 3)))

      // when
    val rangeTree = new RangeTree[SumAggregator](elements)

      // then
    val caught = intercept[Exception] {
        rangeTree.query(3, 2)
      }

    assert(caught.getMessage == "Start (3) greater than end (2)")
  }

  test("RangeTree.init base sum") {
      // given
    val elements = sumWrapInsert(Array((1.0, 0), (2.0, 1), (3.0, 2), (4.0, 3)))

      // when
    val rangeTree = new RangeTree[SumAggregator](elements)

      // then
    assert(4 == rangeTree.BASE)
    assert(sumWrapResult(Array(0.0, 10.0, 3.0, 7.0, 1.0, 2.0, 3.0, 4.0)).sameElements(rangeTree.tree))
  }

  test("RangeTree.init not equal power of two sum") {
      // given
    val elements = sumWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))

      // when
    val rangeTree = new RangeTree[SumAggregator](elements)

      // then
    assert(8 == rangeTree.BASE)
    assert(sumWrapResult(Array(0, 20, 18, 2, 4, 14, 2, 0, 3, 1, 4, 10, 2, 0, 0, 0).map(x => x.toDouble)).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query sum") {
      // given
    val elements = sumWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))

      // when
    val rangeTree = new RangeTree[SumAggregator](elements)

      // then
    assert(new SumAggregator(3.0) == rangeTree.query(0, 0))
    assert(new SumAggregator(8.0) == rangeTree.query(0, 2))
    assert(new SumAggregator(18.0) == rangeTree.query(0, 3))
    assert(new SumAggregator(20.0) == rangeTree.query(0, 4))
    assert(new SumAggregator(14.0) == rangeTree.query(2, 3))
    assert(new SumAggregator(17.0) == rangeTree.query(1, 4))
    assert(new SumAggregator(16.0) == rangeTree.query(2, 5))
  }

  test("RangeTree.init base min") {
      // given
    val elements = minWrapInsert(Array((1.0, 0), (2.0, 1), (3.0, 2), (4.0, 3)))

      // when
    val rangeTree = new RangeTree[MinAggregator](elements)

      // then
    assert(4 == rangeTree.BASE)
    assert(minWrapResult(Array(Double.MaxValue, 1, 1, 3, 1, 2, 3, 4)).sameElements(rangeTree.tree))
  }

  test("RangeTree.init not equal power of two min") {
      // given
    val elements = minWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (-4.0, 2), (10.0, 3)))

      // when
    val rangeTree = new RangeTree[MinAggregator](elements)

      // then
    assert(8 == rangeTree.BASE)
    assert(minWrapResult(Array(Double.MaxValue, -4, -4, 2, 1, -4, 2, Double.MaxValue, 3, 1, -4, 10, 2, Double.MaxValue, Double.MaxValue, Double.MaxValue)).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query min") {
      // given
    val elements = minWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))

      // when
    val rangeTree = new RangeTree[MinAggregator](elements)

      // then
    assert(new MinAggregator(3.0) == rangeTree.query(0, 0))
    assert(new MinAggregator(1.0) == rangeTree.query(0, 2))
    assert(new MinAggregator(1.0) == rangeTree.query(0, 3))
    assert(new MinAggregator(1.0) == rangeTree.query(0, 4))
    assert(new MinAggregator(4.0) == rangeTree.query(2, 3))
    assert(new MinAggregator(1.0) == rangeTree.query(1, 4))
    assert(new MinAggregator(2.0) == rangeTree.query(2, 5))
  }

  test("RangeTree.init base max") {
      // given
    val elements = maxWrapInsert(Array((1.0, 0), (2.0, 1), (3.0, 2), (4.0, 3)))

      // when
    val rangeTree = new RangeTree[MaxAggregator](elements)

      // then
    assert(4 == rangeTree.BASE)
    assert(maxWrapResult(Array(Double.MinValue, 4, 2, 4, 1, 2, 3, 4)).sameElements(rangeTree.tree))
  }

  test("RangeTree.init all negative numbers max") {
      // given
    val elements = maxWrapInsert(Array((-1.0, 0), (-2.0, 1), (-3.0, 2), (-4.0, 3)))

      // when
    val rangeTree = new RangeTree[MaxAggregator](elements)

      // then
    assert(4 == rangeTree.BASE)
    assert(maxWrapResult(Array(Double.MinValue, -1, -1, -3, -1, -2, -3, -4)).sameElements(rangeTree.tree))
  }


  test("RangeTree.init not equal power of two max") {
      // given
    val elements = maxWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))

      // when
    val rangeTree = new RangeTree[MaxAggregator](elements)

      // then
    assert(8 == rangeTree.BASE)
    assert(maxWrapResult(Array(Double.MinValue, 10, 10, 2, 3, 10, 2, Double.MinValue, 3, 1, 4, 10, 2, Double.MinValue, Double.MinValue, Double.MinValue)).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query max") {
      // given
    val elements = maxWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))

      // when
    val rangeTree = new RangeTree[MaxAggregator](elements)

      // then
    assert(new MaxAggregator(3.0) == rangeTree.query(0, 0))
    assert(new MaxAggregator(4.0) == rangeTree.query(0, 2))
    assert(new MaxAggregator(10.0) == rangeTree.query(0, 3))
    assert(new MaxAggregator(10.0) == rangeTree.query(0, 4))
    assert(new MaxAggregator(10.0) == rangeTree.query(2, 3))
    assert(new MaxAggregator(10.0) == rangeTree.query(1, 4))
    assert(new MaxAggregator(10.0) == rangeTree.query(2, 5))
  }
}
