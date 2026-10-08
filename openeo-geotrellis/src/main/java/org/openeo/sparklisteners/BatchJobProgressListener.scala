package org.openeo.sparklisteners;

import org.apache.spark.{Resubmitted, TaskCommitDenied, TaskEndReason, TaskFailedReason, TaskKilled}
import org.apache.spark.scheduler._
import org.openeo.geotrelliscommon.ExecutionMetrics
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable;

object BatchJobProgressListener {
  val logger: Logger = LoggerFactory.getLogger(BatchJobProgressListener.getClass)

  /**
   * A failed task attempt, e.g. an exception, an executor loss or a fetch failure. Excludes attempts that were killed
   * (speculation, cancellation), denied to commit (duplicate attempt) or resubmitted (earlier successful attempt whose
   * output was lost).
   */
  private[sparklisteners] def isTaskFailure(reason: TaskEndReason): Boolean = reason match {
    case _: TaskKilled | _: TaskCommitDenied | Resubmitted => false
    case _: TaskFailedReason => true
    case _ => false
  }

  /**
   * Spark only exposes the removal reason as a string (see org.apache.spark.scheduler.ExecutorLossReason). Removals
   * requested by the driver (e.g. dynamic allocation) and graceful decommissioning are expected; anything else (OOM
   * kill, non-zero exit code, heartbeat timeout, ...) is an unexpected loss.
   */
  private[sparklisteners] def isUnexpectedExecutorLoss(reason: String): Boolean = {
    val r = Option(reason).getOrElse("")
    !(r == "Executor killed by driver." || r.startsWith("Executor decommission:") || r.contains("Finished decommissioning"))
  }
}

class BatchJobProgressListener extends SparkListener {

  import BatchJobProgressListener._

  private val stagesInformation = new mutable.LinkedHashMap[String, mutable.Map[String, Any]]()
  // start time of currently allocated executors
  private val runningExecutors = new mutable.LinkedHashMap[String, Long]
  private var completedExecutorTimeMillis = 0L
  // Executors may have been added before this listener was registered; use this as their start time.
  private var trackingStartTime = System.currentTimeMillis()
  // Only counts stage attempts that failed as a whole (e.g. FetchFailed, aborted stage); task-level retries do not
  // fail a stage.
  private val totalStageFailures = new AtomicInteger(0)
  private val totalTaskFailures = new AtomicInteger(0)
  // max over all tasks of TaskMetrics.peakExecutionMemory
  private val peakExecutionMemoryBytes = new AtomicLong(0L)

  // (stage ID, attempt number) -> executor run time of that stage attempt; used to compute ExecutionMetrics
  // incrementally and thread-safely, independently of stagesInformation above.
  private val stageRuntimes = new ConcurrentHashMap[(Int, Int), java.lang.Long]()
  private val totalStageRuntimeMillis = new AtomicLong(0L)

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = synchronized {
    trackingStartTime = applicationStart.time
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    logger.info(s"Starting stage: ${stageSubmitted.stageInfo.stageId} - ${stageSubmitted.stageInfo.name}. \nStages may combine multiple processes.")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.debug(s"Ending stage: ${stageCompleted.stageInfo.stageId} - ${stageCompleted.stageInfo.name}.")
    val taskMetrics = stageCompleted.stageInfo.taskMetrics
    val stageInformation = new mutable.LinkedHashMap[String, Any]()
    var logs = List[(String, String)]()
    stageInformation += ("duration" -> Duration.ofMillis(taskMetrics.executorRunTime))
    if (stageCompleted.stageInfo.failureReason.isDefined) {
      totalStageFailures.incrementAndGet()
      val message =
        f"""A part of the process graph failed, and will be retried, the reason was: "${stageCompleted.stageInfo.failureReason.get}"
           |Your job may still complete if the failure was caused by a transient error, but will take more time. A common cause of transient errors is too little executor memory (overhead). Too low executor-memory can be seen by a high 'garbage collection' time, which was: ${Duration.ofMillis(taskMetrics.jvmGCTime).toSeconds / 1000.0} seconds.
           |""".stripMargin
      logger.warn(message)
    } else {
      val duration = Duration.ofMillis(taskMetrics.executorRunTime)
      val timeString = if (duration.toSeconds > 60) {
        duration.toMinutes + " m"
      } else {
        duration.toMillis.toFloat / 1000.0 + " s"
      }
      val megabytes = taskMetrics.shuffleWriteMetrics.bytesWritten.toFloat / (1024.0 * 1024.0)
      val name = stageCompleted.stageInfo.name
      val message = f"Stage ${stageCompleted.stageInfo.stageId}: in $timeString - $megabytes%.2f MB  - $name."
      logs = ("info", message) :: logs
      val accumulators = stageCompleted.stageInfo.accumulables;
      val chunkCounts = accumulators.filter(_._2.name.get.startsWith("ChunkCount"));
      if (chunkCounts.nonEmpty) {
        val totalChunks = chunkCounts.head._2.value
        val megapixel = totalChunks.get.asInstanceOf[Long] * 256 * 256 / (1024 * 1024)
        if (taskMetrics.executorRunTime > 0) {
          val messageSpeed = f"load_collection: data was loaded with an average speed of: ${megapixel.toFloat / duration.toSeconds.toFloat}%.3f Megapixel per second."
          logs = ("info", messageSpeed) :: logs
        };
      }
    }
    stageInformation += ("logs" -> logs)
    stagesInformation += (stageCompleted.stageInfo.stageId.toString -> stageInformation)

    val runtimeMillis = taskMetrics.executorRunTime
    val previousRuntime = stageRuntimes.put((stageCompleted.stageInfo.stageId, stageCompleted.stageInfo.attemptNumber()), runtimeMillis)
    totalStageRuntimeMillis.addAndGet(runtimeMillis - Option(previousRuntime).map(_.longValue).getOrElse(0L))
    storeExecutionMetricsIfChanged(stageCompleted.stageInfo.completionTime.getOrElse(System.currentTimeMillis()))
  }


  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskFailed = isTaskFailure(taskEnd.reason)
    if (taskFailed) {
      totalTaskFailures.incrementAndGet()
    }

    val peakMemoryIncreased = Option(taskEnd.taskMetrics).exists { taskMetrics =>
      val previousPeak = peakExecutionMemoryBytes.getAndAccumulate(taskMetrics.peakExecutionMemory, (a: Long, b: Long) => math.max(a, b))
      taskMetrics.peakExecutionMemory > previousPeak
    }

    if (taskFailed || peakMemoryIncreased) {
      storeExecutionMetricsIfChanged(Option(taskEnd.taskInfo).map(_.finishTime).filter(_ > 0).getOrElse(System.currentTimeMillis()))
    }
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    logger.debug(s"Added executor: ${executorAdded.executorId}.")
    if (!runningExecutors.contains(executorAdded.executorId)) {
      runningExecutors += (executorAdded.executorId -> executorAdded.time)
    }
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    if (isUnexpectedExecutorLoss(executorRemoved.reason)) {
      logger.warn(s"Lost executor ${executorRemoved.executorId}: ${executorRemoved.reason}")
    } else {
      logger.debug(s"Removed executor: ${executorRemoved.executorId}: ${executorRemoved.reason}")
    }
    val addedTime = runningExecutors.remove(executorRemoved.executorId).getOrElse(trackingStartTime)
    completedExecutorTimeMillis += math.max(0L, executorRemoved.time - addedTime)
    storeExecutionMetricsIfChanged(executorRemoved.time)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info(s"Application ended: ${applicationEnd.time}.")
    storeExecutionMetricsIfChanged(applicationEnd.time)
    val (totalStages, totalDuration) = stagesInformation.foldLeft((0, Duration.ZERO)) { (x, y) =>
      val duration = y._2.getOrElse("duration", 0) match {
        case n: Duration => n
      }
      (x._1 + 1, x._2.plus(duration))
    }
    val executorTime = synchronized {
      completedExecutorTimeMillis + runningExecutors.values.map(added => math.max(0L, applicationEnd.time - added)).sum
    }
    val executorString = if (executorTime > 60 * 1000) {
      f"${(executorTime / (60 * 1000)).toInt} minutes"
    } else {
      f"${executorTime / 1000} seconds"
    }
    val timeString = if (totalDuration.toMinutes > 5) {
      totalDuration.toMinutes + " minutes"
    } else if (totalDuration.toSeconds > 60) {
      totalDuration.toMinutes + " minutes and " + (totalDuration.toSeconds - 60 * totalDuration.toMinutes) + " seconds"
    } else {
      totalDuration.toMillis.toFloat / 1000.0 + " seconds"
    }
    logger.info(f"Total number of stages: $totalStages")
    logger.info(f"Total stage runtime: $timeString")
    logTopStagesByDuration(totalDuration)
    logger.info(f"Total executor allocation time: $executorString")

    val cpuUtilizationRatio: Double = if (executorTime > 0) {
      totalDuration.toMillis.toDouble / executorTime.toDouble
    } else {
      0d
    }
    logger.info(f"CPU utilization ratio: $cpuUtilizationRatio")
  }

  private def logTopStagesByDuration(totalDuration: Duration): Unit = {
    val ordered = stagesInformation.toSeq.sortWith((a, b) => {
      val DurationA = a._2.getOrElse("duration", 0) match {
        case n: Duration => n
      }
      val DurationB = b._2.getOrElse("duration", 0) match {
        case n: Duration => n
      }
      DurationA.toMillis > DurationB.toMillis
    })
    if (ordered.nonEmpty) {
      logger.info("The following stages are responsible for 80% of the total stage runtime:")
      var tempDuration = 0.0
      var i = 0
      var maxDurationToLog = ordered.head._2.getOrElse("duration", 0) match {
        case n: Duration => n
      }
      while (tempDuration < totalDuration.toMillis * 0.8) {
        val stageInfo = ordered(i)._2
        val logs = stageInfo.getOrElse("logs", "") match {
          case s: List[(String, String)] => s
        }
        for (log <- logs) {
          log match {
            case ("warn", s) => logger.warn(s)
            case ("info", s) => logger.info(s)
          }
        }
        val duration = stageInfo.getOrElse("duration", 0.0) match {
          case v: Duration => v
        }
        tempDuration += duration.toMillis
        maxDurationToLog = duration
        i += 1
      }
    }
  }


  /** Recomputes ExecutionMetrics and stores them if they changed since the last store. */
  private def storeExecutionMetricsIfChanged(now: Long): Unit = {
    val stageRuntimeMillis = totalStageRuntimeMillis.get()
    val executorTimeMillis = synchronized {
      completedExecutorTimeMillis + runningExecutors.values.map(added => math.max(0L, now - added)).sum
    }
    val cpuUtilizationRatio =
      if (executorTimeMillis > 0) stageRuntimeMillis.toDouble / executorTimeMillis.toDouble
      else 0d

    val metrics = ExecutionMetrics(
      totalStageRuntimeMillis = stageRuntimeMillis,
      executorAllocationTimeMillis = executorTimeMillis,
      cpuUtilizationRatio = cpuUtilizationRatio,
      totalStageFailures = totalStageFailures.get(),
      totalTaskFailures = totalTaskFailures.get(),
      peakExecutionMemoryBytes = peakExecutionMemoryBytes.get()
    )

    val previous = ExecutionMetrics.getAndStore(metrics)
    if (metrics != previous) {
      logger.debug(s"Stored $metrics")
    }
  }
}
