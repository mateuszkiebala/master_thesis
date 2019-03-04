package range_tree
import org.scalatest.FunSuite
import minimal_algorithms.RangeTree
import minimal_algorithms.aggregation_function.{MaxAggregation, MinAggregation, SumAggregation}

class RangeTreeTest extends FunSuite {
  test("RangeTree.init one node tree") {
      // given
    val elements = List((1, 0))

      // when
    val rangeTree = new RangeTree(elements, new SumAggregation)

      // then
    assert(1 == rangeTree.BASE)
    assert(Array(0, 1).sameElements(rangeTree.tree))
  }

  test("RangeTree.init position out of range") {
      // given
    val elements = List((1, 0), (2, 4), (3, 2), (4, 3))

      // when, then
    val caught = intercept[Exception] {
        new RangeTree(elements, new SumAggregation)
      }

    assert(caught.getMessage == "Position out of range: 4")
  }

  test("RangeTree.init start > end") {
      // given
    val elements = List((1, 0), (2, 1), (3, 2), (3, 3))

      // when
    val rangeTree = new RangeTree(elements, new SumAggregation)

      // then
    val caught = intercept[Exception] {
        rangeTree.query(3, 2)
      }

    assert(caught.getMessage == "Start (3) greater than end (2)")
  }

  test("RangeTree.init base sum") {
      // given
    val elements = List((1, 0), (2, 1), (3, 2), (4, 3))

      // when
    val rangeTree = new RangeTree(elements, new SumAggregation)

      // then
    assert(4 == rangeTree.BASE)
    assert(Array(0, 10, 3, 7, 1, 2, 3, 4).sameElements(rangeTree.tree))
  }

  test("RangeTree.init not equal power of two sum") {
      // given
    val elements = List((1, 1), (2, 4), (3, 0), (4, 2), (10, 3))

      // when
    val rangeTree = new RangeTree(elements, new SumAggregation)

      // then
    assert(8 == rangeTree.BASE)
    assert(Array(0, 20, 18, 2, 4, 14, 2, 0, 3, 1, 4, 10, 2, 0, 0, 0).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query sum") {
      // given
    val elements = List((1, 1), (2, 4), (3, 0), (4, 2), (10, 3))

      // when
    val rangeTree = new RangeTree(elements, new SumAggregation)

      // then
    assert(3 == rangeTree.query(0, 0))
    assert(8 == rangeTree.query(0, 2))
    assert(18 == rangeTree.query(0, 3))
    assert(20 == rangeTree.query(0, 4))
    assert(14 == rangeTree.query(2, 3))
    assert(17 == rangeTree.query(1, 4))
    assert(16 == rangeTree.query(2, 5))
  }

  test("RangeTree.init base min") {
      // given
    val elements = List((1, 0), (2, 1), (3, 2), (4, 3))

      // when
    val rangeTree = new RangeTree(elements, new MinAggregation)

      // then
    assert(4 == rangeTree.BASE)
    assert(Array(Int.MaxValue, 1, 1, 3, 1, 2, 3, 4).sameElements(rangeTree.tree))
  }

  test("RangeTree.init not equal power of two min") {
      // given
    val elements = List((1, 1), (2, 4), (3, 0), (-4, 2), (10, 3))

      // when
    val rangeTree = new RangeTree(elements, new MinAggregation)

      // then
    assert(8 == rangeTree.BASE)
    assert(Array(Int.MaxValue, -4, -4, 2, 1, -4, 2, Int.MaxValue, 3, 1, -4, 10, 2, Int.MaxValue, Int.MaxValue, Int.MaxValue).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query min") {
      // given
    val elements = List((1, 1), (2, 4), (3, 0), (4, 2), (10, 3))

      // when
    val rangeTree = new RangeTree(elements, new MinAggregation)

      // then
    assert(3 == rangeTree.query(0, 0))
    assert(1 == rangeTree.query(0, 2))
    assert(1 == rangeTree.query(0, 3))
    assert(1 == rangeTree.query(0, 4))
    assert(4 == rangeTree.query(2, 3))
    assert(1 == rangeTree.query(1, 4))
    assert(2 == rangeTree.query(2, 5))
  }

  test("RangeTree.init base max") {
    // given
    val elements = List((1, 0), (2, 1), (3, 2), (4, 3))

    // when
    val rangeTree = new RangeTree(elements, new MaxAggregation)

    // then
    assert(4 == rangeTree.BASE)
    assert(Array(Int.MinValue, 4, 2, 4, 1, 2, 3, 4).sameElements(rangeTree.tree))
  }

  test("RangeTree.init all negative numbers max") {
      // given
    val elements = List((-1, 0), (-2, 1), (-3, 2), (-4, 3))

      // when
    val rangeTree = new RangeTree(elements, new MaxAggregation)

      // then
    assert(4 == rangeTree.BASE)
    assert(Array(Int.MinValue, -1, -1, -3, -1, -2, -3, -4).sameElements(rangeTree.tree))
  }


  test("RangeTree.init not equal power of two max") {
      // given
    val elements = List((1, 1), (2, 4), (3, 0), (4, 2), (10, 3))

      // when
    val rangeTree = new RangeTree(elements, new MaxAggregation)

      // then
    assert(8 == rangeTree.BASE)
    assert(Array(Int.MinValue, 10, 10, 2, 3, 10, 2, Int.MinValue, 3, 1, 4, 10, 2, Int.MinValue, Int.MinValue, Int.MinValue).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query max") {
      // given
    val elements = List((1, 1), (2, 4), (3, 0), (4, 2), (10, 3))

      // when
    val rangeTree = new RangeTree(elements, new MaxAggregation)

      // then
    assert(3 == rangeTree.query(0, 0))
    assert(4 == rangeTree.query(0, 2))
    assert(10 == rangeTree.query(0, 3))
    assert(10 == rangeTree.query(0, 4))
    assert(10 == rangeTree.query(2, 3))
    assert(10 == rangeTree.query(1, 4))
    assert(10 == rangeTree.query(2, 5))
  }
}
