package range_tree
import org.scalatest.FunSuite
import minimal_algorithms.RangeTree

class RangeTreeTest extends FunSuite {
  test("RangeTree.init one node tree") {
      // given
    val elements = List((1, 1))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => x + y, 0)

      // then
    assert(0 == rangeTree.BASE)
    assert(Array(0, 1).sameElements(rangeTree.tree))
  }

  test("RangeTree.init position our of range") {
      // given
    val elements = List((1, 1), (2, 5), (3, 3), (4, 4))

      // when, then
    val caught =
      intercept[Exception] {
        new RangeTree(elements, (x: Int, y: Int) => x + y, 0)
      }

    assert(caught.getMessage == "Position out of range: 5")
  }

  test("RangeTree.init base sum") {
      // given
    val elements = List((1, 1), (2, 2), (3, 3), (4, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => x + y, 0)

      // then
    assert(3 == rangeTree.BASE)
    assert(Array(0, 10, 3, 7, 1, 2, 3, 4).sameElements(rangeTree.tree))
  }

  test("RangeTree.init not equal power of two sum") {
      // given
    val elements = List((1, 2), (2, 5), (3, 1), (4, 3), (10, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => x + y, 0)

      // then
    assert(7 == rangeTree.BASE)
    assert(Array(0, 20, 18, 2, 4, 14, 2, 0, 3, 1, 4, 10, 2, 0, 0, 0).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query sum") {
      // given
    val elements = List((1, 2), (2, 5), (3, 1), (4, 3), (10, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => x + y, 0)

      // then
    assert(3 == rangeTree.query(1, 1))
    assert(8 == rangeTree.query(1, 3))
    assert(18 == rangeTree.query(1, 4))
    assert(20 == rangeTree.query(1, 5))
    assert(14 == rangeTree.query(3, 4))
    assert(17 == rangeTree.query(2, 5))
    assert(16 == rangeTree.query(3, 6))
  }

  test("RangeTree.init base min") {
      // given
    val elements = List((1, 1), (2, 2), (3, 3), (4, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => math.min(x, y), Int.MaxValue)

      // then
    assert(3 == rangeTree.BASE)
    assert(Array(Int.MaxValue, 1, 1, 3, 1, 2, 3, 4).sameElements(rangeTree.tree))
  }

  test("RangeTree.init not equal power of two min") {
      // given
    val elements = List((1, 2), (2, 5), (3, 1), (-4, 3), (10, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => math.min(x, y), Int.MaxValue)

      // then
    assert(7 == rangeTree.BASE)
    assert(Array(Int.MaxValue, -4, -4, 2, 1, -4, 2, Int.MaxValue, 3, 1, -4, 10, 2, Int.MaxValue, Int.MaxValue, Int.MaxValue).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query min") {
      // given
    val elements = List((1, 2), (2, 5), (3, 1), (4, 3), (10, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => math.min(x, y), Int.MaxValue)

      // then
    assert(3 == rangeTree.query(1, 1))
    assert(1 == rangeTree.query(1, 3))
    assert(1 == rangeTree.query(1, 4))
    assert(1 == rangeTree.query(1, 5))
    assert(4 == rangeTree.query(3, 4))
    assert(1 == rangeTree.query(2, 5))
    assert(2 == rangeTree.query(3, 6))
  }

  test("RangeTree.init base max") {
    // given
    val elements = List((1, 1), (2, 2), (3, 3), (4, 4))

    // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => math.max(x, y), Int.MinValue)

    // then
    assert(3 == rangeTree.BASE)
    assert(Array(Int.MinValue, 4, 2, 4, 1, 2, 3, 4).sameElements(rangeTree.tree))
  }

  test("RangeTree.init all negative numbers max") {
      // given
    val elements = List((-1, 1), (-2, 2), (-3, 3), (-4, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => math.max(x, y), Int.MinValue)

      // then
    assert(3 == rangeTree.BASE)
    assert(Array(Int.MinValue, -1, -1, -3, -1, -2, -3, -4).sameElements(rangeTree.tree))
  }


  test("RangeTree.init not equal power of two max") {
      // given
    val elements = List((1, 2), (2, 5), (3, 1), (4, 3), (10, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => math.max(x, y), Int.MinValue)

      // then
    assert(7 == rangeTree.BASE)
    assert(Array(Int.MinValue, 10, 10, 2, 3, 10, 2, Int.MinValue, 3, 1, 4, 10, 2, Int.MinValue, Int.MinValue, Int.MinValue).sameElements(rangeTree.tree))
  }

  test("RangeTree.init query max") {
      // given
    val elements = List((1, 2), (2, 5), (3, 1), (4, 3), (10, 4))

      // when
    val rangeTree = new RangeTree(elements, (x: Int, y: Int) => math.max(x, y), Int.MinValue)

      // then
    assert(3 == rangeTree.query(1, 1))
    assert(4 == rangeTree.query(1, 3))
    assert(10 == rangeTree.query(1, 4))
    assert(10 == rangeTree.query(1, 5))
    assert(10 == rangeTree.query(3, 4))
    assert(10 == rangeTree.query(2, 5))
    assert(10 == rangeTree.query(3, 6))
  }
}
