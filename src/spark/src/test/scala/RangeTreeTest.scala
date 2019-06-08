import minimal_algorithms.RangeTree
import minimal_algorithms.examples.statistics_aggregators.{MaxAggregator, MinAggregator, SumAggregator}
import org.scalatest.FunSuite

class RangeTreeTest extends FunSuite {
  def sumWrapInsert(elements: Array[(Double, Int)]): Array[(SumAggregator, Int)] = {
    elements.map{case(e, pos) => (new SumAggregator(e), pos)}
  }

  def sumUnwrap(elements: Array[SumAggregator]): Array[Double] = {
    elements.map{e => if (e == null) 0.0 else e.getValue}
  }

  def minWrapInsert(elements: Array[(Double, Int)]): Array[(MinAggregator, Int)] = {
    elements.map{case(e, pos) => (new MinAggregator(e), pos)}
  }

  def minUnwrap(elements: Array[MinAggregator]): Array[Double] = {
    elements.map{e => if (e == null) Double.MaxValue else e.getValue}
  }

  def maxWrapInsert(elements: Array[(Double, Int)]): Array[(MaxAggregator, Int)] = {
    elements.map{case(e, pos) => (new MaxAggregator(e), pos)}
  }

  def maxUnwrap(elements: Array[MaxAggregator]): Array[Double] = {
    elements.map{e => if (e == null) Double.MinValue else e.getValue}
  }

  test("RangeTree.init zero nodes tree") {
    val elements = sumWrapInsert(Array())
    val rangeTree = new RangeTree(elements)
    assert(0 == rangeTree.BASE)
    assert(rangeTree.nodes.isEmpty)
  }

  test("RangeTree.init zero nodes tree insert") {
    val elements = sumWrapInsert(Array())
    val rangeTree = new RangeTree(elements)
    val caught = intercept[Exception] {
      rangeTree.insert(new SumAggregator(0), 0)
    }
    assert(caught.getMessage == "Position out of range: 0")
  }

  test("RangeTree.init zero nodes tree insert negative pos") {
    val elements = sumWrapInsert(Array())
    val rangeTree = new RangeTree(elements)
    val caught = intercept[Exception] {
      rangeTree.insert(new SumAggregator(0), -10)
    }
    assert(caught.getMessage == "Position out of range: -10")
  }

  test("RangeTree.init zero nodes tree query") {
    val elements = sumWrapInsert(Array())
    val rangeTree = new RangeTree(elements)
    val caught = intercept[Exception] {
      rangeTree.query(0, 1)
    }
    assert(caught.getMessage == "Position (start) out of range: 0")
  }

  test("RangeTree.init zero nodes tree query negative start") {
    val elements = sumWrapInsert(Array())
    val rangeTree = new RangeTree(elements)
    val caught = intercept[Exception] {
      rangeTree.query(-1, 1)
    }
    assert(caught.getMessage == "Position (start) out of range: -1")
  }

  test("RangeTree.init one node tree") {
    val elements = sumWrapInsert(Array((1.0, 0)))
    val rangeTree = new RangeTree(elements)
    assert(1 == rangeTree.BASE)
    assert(Array(0.0, 1.0).sameElements(sumUnwrap(rangeTree.nodes)))
  }

  test("RangeTree.init position out of range") {
    val elements = sumWrapInsert(Array((1.0, 0), (2.0, 4), (3.0, 2), (4.0, 3)))
    val caught = intercept[Exception] {
        new RangeTree(elements)
      }
    assert(caught.getMessage == "Position out of range: 4")
  }

  test("RangeTree.init start > end") {
    val elements = sumWrapInsert(Array((1.0, 0), (2.0, 1), (3.0, 2), (3.0, 3)))
    val rangeTree = new RangeTree(elements)
    val caught = intercept[Exception] {
        rangeTree.query(3, 2)
      }
    assert(caught.getMessage == "Start (3) greater than end (2)")
  }

  test("RangeTree.init base sum") {
    val elements = sumWrapInsert(Array((1.0, 0), (2.0, 1), (3.0, 2), (4.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(4 == rangeTree.BASE)
    assert(Array(0.0, 10.0, 3.0, 7.0, 1.0, 2.0, 3.0, 4.0).sameElements(sumUnwrap(rangeTree.nodes)))
  }

  test("RangeTree.init not equal power of two sum") {
    val elements = sumWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(8 == rangeTree.BASE)
    assert(Array(0, 20, 18, 2, 4, 14, 2, 0, 3, 1, 4, 10, 2, 0, 0, 0).map(x => x.toDouble).sameElements(sumUnwrap(rangeTree.nodes)))
  }

  test("RangeTree.init query sum") {
    val elements = sumWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(new SumAggregator(3.0) == rangeTree.query(0, 0))
    assert(new SumAggregator(8.0) == rangeTree.query(0, 2))
    assert(new SumAggregator(18.0) == rangeTree.query(0, 3))
    assert(new SumAggregator(20.0) == rangeTree.query(0, 4))
    assert(new SumAggregator(14.0) == rangeTree.query(2, 3))
    assert(new SumAggregator(17.0) == rangeTree.query(1, 4))
    assert(new SumAggregator(16.0) == rangeTree.query(2, 5))
  }

  test("RangeTree.init base min") {
    val elements = minWrapInsert(Array((1.0, 0), (2.0, 1), (3.0, 2), (4.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(4 == rangeTree.BASE)
    assert(Array(Double.MaxValue, 1, 1, 3, 1, 2, 3, 4).sameElements(minUnwrap(rangeTree.nodes)))
  }

  test("RangeTree.init not equal power of two min") {
    val elements = minWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (-4.0, 2), (10.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(8 == rangeTree.BASE)
    assert(Array(Double.MaxValue, -4, -4, 2, 1, -4, 2, Double.MaxValue, 3, 1, -4, 10, 2, Double.MaxValue, Double.MaxValue, Double.MaxValue).sameElements(minUnwrap(rangeTree.nodes)))
  }

  test("RangeTree.init query min") {
    val elements = minWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(new MinAggregator(3.0) == rangeTree.query(0, 0))
    assert(new MinAggregator(1.0) == rangeTree.query(0, 2))
    assert(new MinAggregator(1.0) == rangeTree.query(0, 3))
    assert(new MinAggregator(1.0) == rangeTree.query(0, 4))
    assert(new MinAggregator(4.0) == rangeTree.query(2, 3))
    assert(new MinAggregator(1.0) == rangeTree.query(1, 4))
    assert(new MinAggregator(2.0) == rangeTree.query(2, 5))
  }

  test("RangeTree.init base max") {
    val elements = maxWrapInsert(Array((1.0, 0), (2.0, 1), (3.0, 2), (4.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(4 == rangeTree.BASE)
    assert(Array(Double.MinValue, 4, 2, 4, 1, 2, 3, 4).sameElements(maxUnwrap(rangeTree.nodes)))
  }

  test("RangeTree.init all negative numbers max") {
    val elements = maxWrapInsert(Array((-1.0, 0), (-2.0, 1), (-3.0, 2), (-4.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(4 == rangeTree.BASE)
    assert(Array(Double.MinValue, -1, -1, -3, -1, -2, -3, -4).sameElements(maxUnwrap(rangeTree.nodes)))
  }


  test("RangeTree.init not equal power of two max") {
    val elements = maxWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(8 == rangeTree.BASE)
    assert(Array(Double.MinValue, 10, 10, 2, 3, 10, 2, Double.MinValue, 3, 1, 4, 10, 2, Double.MinValue, Double.MinValue, Double.MinValue).sameElements(maxUnwrap(rangeTree.nodes)))
  }

  test("RangeTree.init query max") {
    val elements = maxWrapInsert(Array((1.0, 1), (2.0, 4), (3.0, 0), (4.0, 2), (10.0, 3)))
    val rangeTree = new RangeTree(elements)
    assert(new MaxAggregator(3.0) == rangeTree.query(0, 0))
    assert(new MaxAggregator(4.0) == rangeTree.query(0, 2))
    assert(new MaxAggregator(10.0) == rangeTree.query(0, 3))
    assert(new MaxAggregator(10.0) == rangeTree.query(0, 4))
    assert(new MaxAggregator(10.0) == rangeTree.query(2, 3))
    assert(new MaxAggregator(10.0) == rangeTree.query(1, 4))
    assert(new MaxAggregator(10.0) == rangeTree.query(2, 5))
  }
}
