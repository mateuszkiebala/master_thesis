package minimal_algorithms.statistics_aggregators

object StatisticsUtils {
  def safeMerge[S <: StatisticsAggregator[S]](a: S, b: S): S = {
    if (a == null) {
      b
    } else if (b == null) {
      a
    } else {
      a.merge(b)
    }
  }

  def scanLeft[S <: StatisticsAggregator[S]](seq: Seq[S], start: S): Seq[S] = {
    if (seq.isEmpty) {
      throw new Exception("Empty sequence")
    } else {
      seq.scanLeft(start){(res, a) => safeMerge(res, a)}
    }
  }

  def scanLeft[S <: StatisticsAggregator[S]](it: Iterator[S], start: S): Iterator[S] = {
    if (it.isEmpty) {
      throw new Exception("Empty iterator")
    } else {
      it.scanLeft(start){(res, a) => safeMerge(res, a)}
    }
  }

  def scanLeft[S <: StatisticsAggregator[S]](seq: Seq[S]): Seq[S] = {
    if (seq.isEmpty) {
      throw new Exception("Empty sequence")
    } else {
      seq.tail.scanLeft(seq.head){(res, a) => safeMerge(res, a)}
    }
  }

  def scanLeft[S <: StatisticsAggregator[S]](it: Iterator[S]): Iterator[S] = {
    if (it.isEmpty) {
      throw new Exception("Empty iterator")
    } else {
      val head = it.next
      it.scanLeft(head){(res, a) => safeMerge(res, a)}
    }
  }

  def foldLeft[S <: StatisticsAggregator[S]](seq: Seq[S], start: S): S = {
    if (seq.isEmpty) {
      throw new Exception("Empty sequence")
    } else {
      seq.foldLeft(start){(res, a) => safeMerge(res, a)}
    }
  }

  def foldLeft[S <: StatisticsAggregator[S]](it: Iterator[S], start: S): S = {
    if (it.isEmpty) {
      throw new Exception("Empty iterator")
    } else {
      it.foldLeft(start){(res, a) => safeMerge(res, a)}
    }
  }

  def foldLeft[S <: StatisticsAggregator[S]](seq: Seq[S]): S = {
    if (seq.isEmpty) {
      throw new Exception("Empty sequence")
    } else {
      seq.tail.foldLeft(seq.head){(res, a) => safeMerge(res, a)}
    }
  }

  def foldLeft[S <: StatisticsAggregator[S]](it: Iterator[S]): S = {
    if (it.isEmpty) {
      throw new Exception("Empty iterator")
    } else {
      val head = it.next
      it.foldLeft(head){(res, a) => safeMerge(res, a)}
    }
  }
}
