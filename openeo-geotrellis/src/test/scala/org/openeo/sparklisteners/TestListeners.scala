package org.openeo.sparklisteners

import org.junit.jupiter.api.{Disabled, Test}
import org.openeo.geotrellis.LocalSparkContext

object TestListeners {}

class TestListeners extends LocalSparkContext {

  @Disabled("For debugging.")
  @Test
  def testBatchJobProgressListener(): Unit = {

    val listener = new BatchJobProgressListener()
    sc.addSparkListener(listener)

    var rdd = sc.parallelize(1 to 5)
    rdd = rdd.map { i => throw new java.lang.Exception(); i + 10 }
    try {
      rdd.collect()
    } catch {
      case e: Exception => println(e)
    }
    println("done")
  }
}
