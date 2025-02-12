package org.openeo.sparklisteners;

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.util.AccumulatorV2
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import java.time.Duration;

object BatchJobProgressListener {

    val logger = LoggerFactory.getLogger(BatchJobProgressListener.getClass)

}

class BatchJobProgressListener extends SparkListener {

    import BatchJobProgressListener.logger

    private val stagesInformation = new mutable.LinkedHashMap[String,mutable.Map[String,Any]]()
    private val executorInformation = new mutable.LinkedHashMap[String,(Long,Long)]

    override def onStageSubmitted( stageSubmitted:SparkListenerStageSubmitted):Unit = {
        logger.info(s"Starting stage: ${stageSubmitted.stageInfo.stageId} - ${stageSubmitted.stageInfo.name}. \nStages may combine multiple processes." )
    }

   override def onStageCompleted( stageCompleted: SparkListenerStageCompleted):Unit = {
        val taskMetrics = stageCompleted.stageInfo.taskMetrics
        val stageInformation = new mutable.LinkedHashMap[String,Any]()
        var logs = List[(String, String)]()
        stageInformation += ("duration" -> Duration.ofMillis(taskMetrics.executorRunTime))
        if(stageCompleted.stageInfo.failureReason.isDefined){
          val message =
            f"""A part of the process graph failed, and will be retried, the reason was: "${stageCompleted.stageInfo.failureReason.get}"
               |Your job may still complete if the failure was caused by a transient error, but will take more time. A common cause of transient errors is too little executor memory (overhead). Too low executor-memory can be seen by a high 'garbage collection' time, which was: ${Duration.ofMillis(taskMetrics.jvmGCTime).toSeconds / 1000.0} seconds.
               |""".stripMargin
          logs = ("warn", message) :: logs

        }else{
          val duration = Duration.ofMillis(taskMetrics.executorRunTime)
          val timeString = if(duration.toSeconds>60) {
            duration.toMinutes + " m"
          } else {
            duration.toMillis.toFloat / 1000.0 + " s"
          }
          val megabytes = taskMetrics.shuffleWriteMetrics.bytesWritten.toFloat/(1024.0*1024.0)
          val name = stageCompleted.stageInfo.name
          val message = f"Stage ${stageCompleted.stageInfo.stageId}: in $timeString - $megabytes%.2f MB  - $name."
          logs = ("info",message) :: logs
          val accumulators = stageCompleted.stageInfo.accumulables;
          val chunkCounts = accumulators.filter(_._2.name.get.startsWith("ChunkCount"));
          if (chunkCounts.nonEmpty) {
            val totalChunks = chunkCounts.head._2.value
            val megapixel = totalChunks.get.asInstanceOf[Long] * 256 * 256 / (1024 * 1024)
            if(taskMetrics.executorRunTime > 0) {
              val messageSpeed = f"load_collection: data was loaded with an average speed of: ${megapixel.toFloat/ duration.toSeconds.toFloat}%.3f Megapixel per second."
              logs = ("info",messageSpeed) :: logs
            };
          }
        }
        stageInformation += ("logs" -> logs)
        stagesInformation += (stageCompleted.stageInfo.stageId.toString -> stageInformation)

    }


  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    val time = executorAdded.time
    val executorId = executorAdded.executorId
    if (executorInformation.contains(executorId)){
      val (addedTime,removedTime) = executorInformation(executorId)
      executorInformation += (executorId -> (addedTime+time,removedTime))
    } else executorInformation += (executorId -> (time,0L))

  }
  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    val time = executorRemoved.time
    val executorId = executorRemoved.executorId
    if(executorInformation.contains(executorId)){
      val (addedTime,removedTime) = executorInformation(executorId)
      executorInformation += (executorId -> (addedTime,removedTime))
    }else executorInformation += (executorId ->(0L,time))
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd):Unit={
    val (totalStages, totalDuration) = stagesInformation.foldLeft((0,Duration.ZERO)){ (x,y) =>
      val duration = y._2.getOrElse("duration",0) match {
        case n:Duration => n
      }
      (x._1 + 1, x._2.plus(duration))
    }
    val executorTime = executorInformation.foldLeft(0L)((x,y) => {
      val (_,(added,removed)) = y
      x + added - removed
    })
    val executorString = if (executorTime > 60*1000 ){
      f"${(executorTime /(60*1000)).toInt} minutes"
    }else{
      f"${executorTime / 1000} seconds"
    }
    val ordered = stagesInformation.toSeq.sortWith((a,b) =>{
      val DurationA = a._2.getOrElse("duration",0) match {case n:Duration => n}
      val DurationB = b._2.getOrElse("duration",0) match {case n:Duration => n}
      DurationA.toMillis > DurationB.toMillis
    })
    val timeString = if(totalDuration.toMinutes>5) {
      totalDuration.toMinutes + " minutes"
    } else if(totalDuration.toSeconds > 60) {
      totalDuration.toMinutes + " minutes and " + (totalDuration.toSeconds-60*totalDuration.toMinutes) + " seconds"
    } else {
      totalDuration.toMillis.toFloat / 1000.0 + " seconds"
    }
    logger.info(f"Summary of the executed stages with the Logs of the longest stages:")
    logger.info(f"Total number of stages: $totalStages")
    logger.info(f"Total stage runtime: $timeString")
    logger.info(f"Total executor allocation time: $executorString")
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
}
