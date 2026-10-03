package org.openeo.sparklisteners;

import org.apache.spark.scheduler._
import org.openeo.geotrelliscommon.ExecutionMetrics
import org.slf4j.{Logger, LoggerFactory}

import java.lang.management.ManagementFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable;

object BatchJobProgressListener {

  val logger: Logger = LoggerFactory.getLogger(BatchJobProgressListener.getClass)
}

class BatchJobProgressListener extends SparkListener {

  import BatchJobProgressListener.logger

  private val stagesInformation = new mutable.LinkedHashMap[String, mutable.Map[String, Any]]()
  // start time of currently allocated executors
  private val runningExecutors = new mutable.LinkedHashMap[String, Long]
  private var completedExecutorTimeMillis = 0L
  // Executors may have been added before this listener was registered; use this as their start time.
  private var trackingStartTime = System.currentTimeMillis()
  private val totalStageFailures = new AtomicInteger(0)

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
    val taskMetrics = stageCompleted.stageInfo.taskMetrics
    val stageInformation = new mutable.LinkedHashMap[String, Any]()
    var logs = List[(String, String)]()
    stageInformation += ("duration" -> Duration.ofMillis(taskMetrics.executorRunTime))
    if (stageCompleted.stageInfo.failureReason.isDefined) {
      val message =
        f"""A part of the process graph failed, and will be retried, the reason was: "${stageCompleted.stageInfo.failureReason.get}"
           |Your job may still complete if the failure was caused by a transient error, but will take more time. A common cause of transient errors is too little executor memory (overhead). Too low executor-memory can be seen by a high 'garbage collection' time, which was: ${Duration.ofMillis(taskMetrics.jvmGCTime).toSeconds / 1000.0} seconds.
           |""".stripMargin
      logs = ("warn", message) :: logs
      totalStageFailures.incrementAndGet()

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
    logger.debug(s"BatchJobProgressListener.onStageCompleted() called in JVM process ${ManagementFactory.getRuntimeMXBean.getName}")

    val runtimeMillis = taskMetrics.executorRunTime
    val previousRuntime = stageRuntimes.put((stageCompleted.stageInfo.stageId, stageCompleted.stageInfo.attemptNumber()), runtimeMillis)
    totalStageRuntimeMillis.addAndGet(runtimeMillis - Option(previousRuntime).map(_.longValue).getOrElse(0L))
    storeExecutionMetricsIfChanged(stageCompleted.stageInfo.completionTime.getOrElse(System.currentTimeMillis()))
  }


  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    if (!runningExecutors.contains(executorAdded.executorId)) {
      runningExecutors += (executorAdded.executorId -> executorAdded.time)
    }
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    val addedTime = runningExecutors.remove(executorRemoved.executorId).getOrElse(trackingStartTime)
    completedExecutorTimeMillis += math.max(0L, executorRemoved.time - addedTime)
    storeExecutionMetricsIfChanged(executorRemoved.time)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
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
    val ordered = stagesInformation.toSeq.sortWith((a, b) => {
      val DurationA = a._2.getOrElse("duration", 0) match {
        case n: Duration => n
      }
      val DurationB = b._2.getOrElse("duration", 0) match {
        case n: Duration => n
      }
      DurationA.toMillis > DurationB.toMillis
    })
    val timeString = if (totalDuration.toMinutes > 5) {
      totalDuration.toMinutes + " minutes"
    } else if (totalDuration.toSeconds > 60) {
      totalDuration.toMinutes + " minutes and " + (totalDuration.toSeconds - 60 * totalDuration.toMinutes) + " seconds"
    } else {
      totalDuration.toMillis.toFloat / 1000.0 + " seconds"
    }
    logger.info(f"Summary of the executed stages with the Logs of the longest stages:")
    logger.info(f"Total number of stages: $totalStages")
    logger.info(f"Total stage runtime: $timeString")
    logger.info(f"Total executor allocation time: $executorString")

    val cpuUtilizationRatio: Double = if (executorTime > 0) {
      totalDuration.toMillis.toDouble / executorTime.toDouble
    } else {
      0d
    }
    logger.info(f"CPU utilization ratio: $cpuUtilizationRatio")


    storeExecutionMetricsIfChanged(applicationEnd.time)

    if (totalStages > 0) {
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
      totalStageFailures = totalStageFailures.get()
    )

    val previous = ExecutionMetrics.getAndStore(metrics)
    if (metrics != previous) {
      logger.debug(s"Stored $metrics")
    }
  }
}
