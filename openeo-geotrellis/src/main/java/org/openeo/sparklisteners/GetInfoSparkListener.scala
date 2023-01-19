package org.openeo.sparklisteners

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}


class GetInfoSparkListener extends SparkListener {
  private val jobsCompleted = new AtomicInteger(0)
  private val stagesCompleted = new AtomicInteger(0)
  private val tasksCompleted = new AtomicInteger(0)
  private val executorRuntime = new AtomicLong(0L)
  private val recordsRead = new AtomicLong(0L)
  private val recordsWritten = new AtomicLong(0L)

  def getStagesCompleted: Int = stagesCompleted.get()

  def getTasksCompleted: Int = tasksCompleted.get()

  def printStatus(): Unit = {
    println("***************** Aggregate metrics *****************************")
    println("* jobsCompleted: " + jobsCompleted)
    println("* stagesCompleted: " + stagesCompleted)
    println("* tasksCompleted: " + tasksCompleted)
    println("* executorRuntime: " + executorRuntime)
    println("* recordsRead: " + recordsRead)
    println("* recordsWritten: " + recordsWritten)
    println("*****************************************************************")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("GetInfoSparkListener.onApplicationEnd(...)")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val newValue = jobsCompleted.incrementAndGet()
    println("GetInfoSparkListener.onJobEnd(...) jobsCompleted: " + newValue)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val newValue = stagesCompleted.incrementAndGet()
    println("GetInfoSparkListener.onStageCompleted(...) stagesCompleted: " + newValue)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val newValue = tasksCompleted.incrementAndGet()
    println("GetInfoSparkListener.onTaskEnd(...) tasksCompleted: " + newValue)
    executorRuntime.addAndGet(taskEnd.taskMetrics.executorRunTime)
    recordsRead.addAndGet(taskEnd.taskMetrics.inputMetrics.recordsRead)
    recordsWritten.addAndGet(taskEnd.taskMetrics.outputMetrics.recordsWritten)
  }
}
