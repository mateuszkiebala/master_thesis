package minimal_algorithms.spark.statistics

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

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

  /**
    * Computes prefix values on partitions' statistics
    * @param rdd  RDD of objects
    * @return Array of prefix statistics for partitions
    */
  def prefixedPartitionStatistics[A, S <: StatisticsAggregator[S]]
  (rdd: RDD[A], statsAgg: A => S)(implicit atag: ClassTag[A], stag: ClassTag[S]): Seq[S] = {
    val elements = partitionStatistics(rdd, statsAgg).collect()
    if (elements.isEmpty) Seq[S]() else scanLeft(elements)
  }

  /**
    * Computes aggregated values for each partition.
    * @param rdd  Elements
    * @return RDD[aggregated value for partition]
    */
  def partitionStatistics[A, S <: StatisticsAggregator[S]]
  (rdd: RDD[A], statsAgg: A => S)(implicit atag: ClassTag[A], stag: ClassTag[S]): RDD[S] = {
    rdd.mapPartitions(partitionIt => {
      if (partitionIt.isEmpty) {
        Iterator()
      } else {
        val start = partitionIt.next
        Iterator(partitionIt.foldLeft(statsAgg(start)){(acc, o) => acc.merge(statsAgg(o))})
      }
    })
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
