package org.openeo.sparklisteners;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.util.AccumulatorV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Traversable;
import scala.collection.mutable.Map;

import java.time.Duration;

object BatchJobProgressListener {

    val logger = LoggerFactory.getLogger(BatchJobProgressListener.getClass)

}

class BatchJobProgressListener extends SparkListener {

    import BatchJobProgressListener.logger

    override def onStageSubmitted( stageSubmitted:SparkListenerStageSubmitted):Unit = {
        logger.info("Starting part of the process graph: " + stageSubmitted.stageInfo.name)
    }

   override def onStageCompleted( stageCompleted: SparkListenerStageCompleted):Unit = {
        val taskMetrics = stageCompleted.stageInfo.taskMetrics

        if(stageCompleted.stageInfo.failureReason.isDefined){
            logger.warn("A part of the process graph failed, and will be retried, the reason was: " + stageCompleted.stageInfo.failureReason.get);
            logger.info("Your job may still complete if the failure was caused by a transient error, but will take more time. A common cause of transient errors is too little executor memory (overhead). Too low executor-memory can be seen by a high 'garbage collection' time, which was: " + Duration.ofNanos(taskMetrics.jvmGCTime).toSeconds + " seconds.");
        }else{
            logger.info("Finished part of the process graph: " + stageCompleted.stageInfo.name + ".\n The total computing time was: " + Duration.ofNanos(taskMetrics.executorRunTime).toMinutes + " minutes. It produced: " +taskMetrics.outputMetrics.bytesWritten/(1024*1024) + "MB of data.");
        }

        val accumulators = stageCompleted.stageInfo.accumulables;
        val chunkCounts = accumulators.filter(_._2.name.get.startsWith("ChunkCount"));
        if (chunkCounts.nonEmpty) {
            val totalChunks = chunkCounts.head._2.value
            val megapixel = totalChunks.get.asInstanceOf[Long] * 256 * 256 / (1024 * 1024)
            if(taskMetrics.executorRunTime > 0) {
              logger.info("load_collection: data was loaded with an average speed of :" + megapixel/ Duration.ofNanos(taskMetrics.executorRunTime).toSeconds() + "Megapixel per second.")
            };
        }
    }


}
