package org.openeo.sparklisteners

import org.apache.spark.{SparkContext, TaskFailedReason}
import org.apache.spark.scheduler._
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

object LogErrorSparkListener {
  private val logger = LoggerFactory.getLogger(LogErrorSparkListener.getClass)
  private var listener: Option[LogErrorSparkListener] = None

  def assureListening(implicit sc: SparkContext): Unit = {
    if (listener.isEmpty) {
      // A JVM can only have one Spark listener at the same time.
      val l = new LogErrorSparkListener()
      sc.addSparkListener(l)
      listener = Some(l)
    }
  }

  private def removeListener(): Unit = {
    if (listener.isDefined) {
      SparkContext.getOrCreate().removeSparkListener(listener.get)
      listener = None
    }
  }

  def extractPythonError(stackTrace: String): Option[String] = {
    val tracaback = "Traceback (most recent call last):"
    val idx = stackTrace.indexOf(tracaback)
    if (idx < 0) {
      return None
    }
    val s = stackTrace.substring(idx + tracaback.length)
    val lines = s.split("\n")
    val lineStart = lines.indexWhere(l => l.nonEmpty && !" \t".contains(l(0)))
    var lineEnd = lines.indexWhere(l => l.startsWith("   at "), lineStart)
    if (lineEnd == -1) lineEnd = lines.length - 1
    Some(lines.slice(lineStart, lineEnd).mkString("\n").trim)
  }
}

class LogErrorSparkListener extends SparkListener {
  private val jobsCompleted = new AtomicInteger(0)
  private val stagesCompleted = new AtomicInteger(0)
  private val tasksCompleted = new AtomicInteger(0)
  private val executorRuntime = new AtomicLong(0L)
  private val recordsRead = new AtomicLong(0L)
  private val recordsWritten = new AtomicLong(0L)
  private val debug = false

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
    if (debug) println("LogErrorSparkListener.onApplicationEnd(...)")
    // No info to log from applicationEnd

    if (LogErrorSparkListener.listener.isDefined) {
      // Not sure if the spark context sometimes get stopped and started in the same JVM, but just in case...
      println("Removing LogErrorSparkListener, if a SparkContext will be re-made in this JVM, a listener needs to be re-attached")
      LogErrorSparkListener.removeListener()
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val newValue = jobsCompleted.incrementAndGet()
    if (debug) println("LogErrorSparkListener.onJobEnd(...) jobsCompleted: " + newValue)
    //    jobEnd.jobResult match {
    //      case j: JobFailed => LogErrorSparkListener.logger.error(j) // JobFailed is private in Spark API
    //    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val newValue = stagesCompleted.incrementAndGet()
    if (debug) println("LogErrorSparkListener.onStageCompleted(...) stagesCompleted: " + newValue)
    stageCompleted.stageInfo.failureReason foreach {
      x => LogErrorSparkListener.logger.error("LogErrorSparkListener.onStageCompleted(...) error: " + x)
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val newValue = tasksCompleted.incrementAndGet()
    if (debug) println("LogErrorSparkListener.onTaskEnd(...) tasksCompleted: " + newValue)
    taskEnd.reason match {
      case r: TaskFailedReason => LogErrorSparkListener.extractPythonError(r.toErrorString).foreach(
        m => LogErrorSparkListener.logger.warn("LogErrorSparkListener.onTaskEnd(...) error: " + m)
      )
      case _ => // Ignore
    }

    // taskMetrics may be null if the task has failed
    if (taskEnd.taskMetrics != null) {
      executorRuntime.addAndGet(taskEnd.taskMetrics.executorRunTime)
      recordsRead.addAndGet(taskEnd.taskMetrics.inputMetrics.recordsRead)
      recordsWritten.addAndGet(taskEnd.taskMetrics.outputMetrics.recordsWritten)
    }
  }
}
