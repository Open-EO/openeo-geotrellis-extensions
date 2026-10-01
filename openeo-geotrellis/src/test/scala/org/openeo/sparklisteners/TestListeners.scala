package org.openeo.sparklisteners

import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{Disabled, Test}
import org.openeo.geotrellis.LocalSparkContext
import org.openeo.geotrelliscommon.ExecutionMetrics
import scala.collection.immutable.Map

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

  @Test
  def testUsageMetricsAreStoredOnApplicationEnd(): Unit = {
    val listener = new BatchJobProgressListener()
    val startedAt = System.currentTimeMillis()

    listener.onExecutorAdded(SparkListenerExecutorAdded(
      startedAt,
      "executor-1",
      new ExecutorInfo("localhost", 1, Map.empty[String, String])
    ))
    val stageInfo = buildStageInfo(startedAt, 2500L)
    callStageCallback(listener, "onStageSubmitted", "org.apache.spark.scheduler.SparkListenerStageSubmitted", stageInfo, new java.util.Properties())
    callStageCallback(listener, "onStageCompleted", "org.apache.spark.scheduler.SparkListenerStageCompleted", stageInfo)
    listener.onExecutorRemoved(SparkListenerExecutorRemoved(startedAt + 2500L, "executor-1", "test"))
    listener.onApplicationEnd(SparkListenerApplicationEnd(startedAt + 5000L))

    assertEquals(ExecutionMetrics(2500L, 2500L, 1.0, 0), ExecutionMetrics.get)
  }

  @Test
  def testBatchJobProgressListenerStoresMetricsOnChange(): Unit = {
    val listener = new BatchJobProgressListener()
    val startedAt = System.currentTimeMillis()

    listener.onExecutorAdded(SparkListenerExecutorAdded(
      startedAt,
      "executor-1",
      new ExecutorInfo("localhost", 1, Map.empty[String, String])
    ))
    val stageInfo = buildStageInfo(startedAt, 2000L)
    callStageCallback(listener, "onStageSubmitted", "org.apache.spark.scheduler.SparkListenerStageSubmitted", stageInfo, new java.util.Properties())
    callStageCallback(listener, "onStageCompleted", "org.apache.spark.scheduler.SparkListenerStageCompleted", stageInfo)
    assertEquals(ExecutionMetrics(2000L, 2000L, 1.0, 0), ExecutionMetrics.get)

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(startedAt + 4000L, "executor-1", "test"))
    assertEquals(ExecutionMetrics(2000L, 4000L, 0.5, 0), ExecutionMetrics.get)

    listener.onApplicationEnd(SparkListenerApplicationEnd(startedAt + 5000L))
    assertEquals(ExecutionMetrics(2000L, 4000L, 0.5, 0), ExecutionMetrics.get)
  }

  private def stageInfoDefault(methodName: String): Any = {
    val stageInfoCompanion = Class.forName("org.apache.spark.scheduler.StageInfo$")
    val module = stageInfoCompanion.getField("MODULE$").get(null)
    stageInfoCompanion.getMethod(methodName).invoke(module)
  }

  private def buildStageInfo(startedAt: Long, durationMillis: Long): AnyRef = {
    val taskMetricsClass = Class.forName("org.apache.spark.executor.TaskMetrics")
    val taskMetrics = taskMetricsClass.getDeclaredConstructor().newInstance()
    taskMetricsClass.getMethod("setExecutorRunTime", classOf[Long]).invoke(taskMetrics, Long.box(durationMillis))
    taskMetricsClass.getMethod("setJvmGCTime", classOf[Long]).invoke(taskMetrics, Long.box(0L))

    val stageInfo = Class.forName("org.apache.spark.scheduler.StageInfo")
      .getConstructors
      .head
      .newInstance(
        Int.box(1),
        Int.box(0),
        "test-stage",
        Int.box(1),
        Seq.empty,
        Seq.empty,
        "test-details",
        taskMetrics,
        stageInfoDefault("$lessinit$greater$default$9").asInstanceOf[AnyRef],
        stageInfoDefault("$lessinit$greater$default$10").asInstanceOf[AnyRef],
        Int.box(0),
        java.lang.Boolean.valueOf(stageInfoDefault("$lessinit$greater$default$12").asInstanceOf[Boolean]),
        Int.box(stageInfoDefault("$lessinit$greater$default$13").asInstanceOf[Int]),
      ).asInstanceOf[AnyRef]

    stageInfo.getClass.getMethod("completionTime_$eq", classOf[Option[Object]])
      .invoke(stageInfo, Some(Long.box(startedAt + durationMillis)))
    stageInfo
  }

  private def callStageCallback(listener: SparkListener, callback: String, eventClassName: String, args: AnyRef*): Unit = {
    val eventClass = Class.forName(eventClassName)
    val constructor = eventClass.getConstructors.find(_.getParameterCount == args.size).get
    val event = constructor.newInstance(args: _*)
    listener.getClass.getMethod(callback, eventClass).invoke(listener, event)
  }

  private def fail(message: String): Nothing = throw new AssertionError(message)
}
