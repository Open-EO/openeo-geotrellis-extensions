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
        logger.info("Starting part of the process graph: " + stageSubmitted.stageInfo.details)
    }

   override def onStageCompleted( stageCompleted: SparkListenerStageCompleted):Unit = {
        val taskMetrics = stageCompleted.stageInfo.taskMetrics

        if(stageCompleted.stageInfo.failureReason.isDefined){
            logger.warn("A part of the process graph failed, and will be retried, the reason was: " + stageCompleted.stageInfo.failureReason.get);
            logger.info("Your job may still complete if the failure was caused by a transient error, but will take more time. A common cause of transient errors is too little executor memory (overhead). Too low executor-memory can be seen by a high 'garbage collection' time, which was: " + Duration.ofNanos(taskMetrics.jvmGCTime).toSeconds + " seconds.");
        }else{
            logger.info("Finished part of the process graph: " + stageCompleted.stageInfo.details + ".\n The total computing time was: " + Duration.ofNanos(taskMetrics.executorCpuTime).toMinutes + " minutes. It produced: " +taskMetrics.shuffleWriteMetrics.bytesWritten/(1024*1024) + "MB of data.");
        }


        //below would be nice, but depends on private api!
        /*val accumulators = taskMetrics.nameToAccums();
        Traversable<String> chunkCounts = accumulators.keys().filter(key -> key.startsWith("ChunkCount"));
        if (chunkCounts.nonEmpty()) {
            Long totalChunks = (Long) accumulators.get(chunkCounts.head()).get().value();
            Long megapixel = totalChunks * 256 * 256 / (1024 * 1024);
            logger.info("load_collection: data was loaded with an average speed of :" + megapixel/ Duration.ofNanos(taskMetrics.executorCpuTime()).toSeconds() + "Megapixel per second.");
        }*/
    }


}
