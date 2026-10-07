package org.openeo.sparklisteners

import org.apache.spark.{ExecutorLostFailure, Resubmitted, Success, TaskEndReason, TaskKilled, TaskResultLost, UnknownReason}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, SparkListenerTaskEnd}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
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

    assertEquals(ExecutionMetrics(2500L, 2500L, 1.0, 0, 0), ExecutionMetrics.get)
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
    assertEquals(ExecutionMetrics(2000L, 2000L, 1.0, 0, 0), ExecutionMetrics.get)

    listener.onExecutorRemoved(SparkListenerExecutorRemoved(startedAt + 4000L, "executor-1", "test"))
    assertEquals(ExecutionMetrics(2000L, 4000L, 0.5, 0, 0), ExecutionMetrics.get)

    listener.onApplicationEnd(SparkListenerApplicationEnd(startedAt + 5000L))
    assertEquals(ExecutionMetrics(2000L, 4000L, 0.5, 0, 0), ExecutionMetrics.get)
  }

  @Test
  def testTaskFailuresAreCounted(): Unit = {
    val listener = new BatchJobProgressListener()
    ExecutionMetrics.store(ExecutionMetrics(0L, 0L, 0d, 0, 0))

    def taskEnd(reason: TaskEndReason): SparkListenerTaskEnd =
      SparkListenerTaskEnd(1, 0, "ResultTask", reason, null, null, null)

    listener.onTaskEnd(taskEnd(Success))
    listener.onTaskEnd(taskEnd(TaskKilled("speculation")))
    listener.onTaskEnd(taskEnd(Resubmitted))
    assertEquals(0, ExecutionMetrics.get.totalTaskFailures)

    listener.onTaskEnd(taskEnd(ExecutorLostFailure("executor-1", exitCausedByApp = false, Some("container preempted"))))
    listener.onTaskEnd(taskEnd(TaskResultLost))
    listener.onTaskEnd(taskEnd(UnknownReason))
    assertEquals(3, ExecutionMetrics.get.totalTaskFailures)
    assertEquals(0, ExecutionMetrics.get.totalStageFailures)
  }

  @Test
  def testFailuresOfSparkJobAreCounted(): Unit = {
    ExecutionMetrics.store(ExecutionMetrics(0L, 0L, 0d, 0, 0))
    sc.addSparkListener(new BatchJobProgressListener())

    try {
      sc.parallelize(Seq(1), numSlices = 1).map(_ => throw new IllegalStateException("boom")).collect()
      fail("job should have failed")
    } catch {
      case _: org.apache.spark.SparkException =>
    }

    // listener events are delivered asynchronously
    val deadline = System.currentTimeMillis() + 10000L
    while (ExecutionMetrics.get.totalStageFailures == 0 && System.currentTimeMillis() < deadline) Thread.sleep(50)

    assertEquals(1, ExecutionMetrics.get.totalTaskFailures)
    assertEquals(1, ExecutionMetrics.get.totalStageFailures)
  }

  @Test
  def testUnexpectedExecutorLoss(): Unit = {
    import BatchJobProgressListener.isUnexpectedExecutorLoss
    assertFalse(isUnexpectedExecutorLoss("Executor killed by driver."))
    assertFalse(isUnexpectedExecutorLoss("Executor decommission: worker decommissioned"))
    assertFalse(isUnexpectedExecutorLoss("Finished decommissioning"))
    assertTrue(isUnexpectedExecutorLoss("Executor Process Lost"))
    assertTrue(isUnexpectedExecutorLoss("Command exited with code 137"))
    assertTrue(isUnexpectedExecutorLoss(null))
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
