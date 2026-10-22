package org.apache.spark

object SparkTestHelper {
  def waitUntilListenerBusEmpty(sc: SparkContext): Unit = sc.listenerBus.waitUntilEmpty()
}
