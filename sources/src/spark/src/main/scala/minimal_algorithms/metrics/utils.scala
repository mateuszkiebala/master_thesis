package minimal_algorithms.spark.metrics

import java.io._
import java.util.concurrent._

object Utils {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (TimeUnit.NANOSECONDS.toMillis(t1 - t0) + "ms"))
    result
  }
}
