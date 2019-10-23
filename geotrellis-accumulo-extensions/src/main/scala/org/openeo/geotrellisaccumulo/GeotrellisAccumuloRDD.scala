/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openeo.geotrellisaccumulo

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.accumulo.core.client.impl.DelegationTokenImpl
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.impl.BatchInputSplit
import org.apache.accumulo.core.client.mapreduce.lib.impl.ConfiguratorBase
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{JobConf, SplitLocationInfo}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.{JobContextImpl, TaskAttemptContextImpl}
import org.apache.hadoop.security.token.Token
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class NewHadoopPartition(
    rddId: Int,
    val index: Int,
    rawSplit: InputSplit with Writable)
  extends Partition {

  val serializableHadoopSplit = new SerializableWritable(rawSplit)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

private class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    value = new Configuration(false)
    value.readFields(in)
  }
}

/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the new MapReduce API (`org.apache.hadoop.mapreduce`).
 *
 * Note: Instantiating this class directly is not recommended, please use
 * [[org.apache.spark.SparkContext.newAPIHadoopRDD()]]
 *
 * @param sc The SparkContext to associate the RDD with.
 */

class GeotrellisAccumuloRDD(
    sc : SparkContext,
    @transient private val _conf: Configuration,
    private val splitRanges: Boolean = false)
  extends RDD[(Key, Value)](sc, Nil)  {

  // A Hadoop Configuration can be about 10 KB, which is pretty big, so broadcast it
  private val confBroadcast = sc.broadcast(new SerializableConfiguration(_conf))
  // private val serializableConf = new SerializableWritable(_conf)

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  @transient protected val jobId = new JobID(jobTrackerId, id)
/*

*/
  def getConf: Configuration = {
    val conf: Configuration = confBroadcast.value.value
    conf
  }



  override def getPartitions: Array[Partition] = {
    val inputFormat = new AccumuloInputFormat()

    val jobContext = new JobContextImpl(getConf, jobId)
    SparkHadoopUtil.get.addCredentials(jobContext.getConfiguration.asInstanceOf[JobConf])

    val token = ConfiguratorBase.getAuthenticationToken(classOf[AccumuloInputFormat], jobContext.getConfiguration)
    if(token.isInstanceOf[DelegationTokenImpl]) {
      //normally the sparkhadooputil should have configured our token...
      println("Configuring delegationtoken directly")
      val delegationToken = token.asInstanceOf[DelegationTokenImpl]
      val identifier = delegationToken.getIdentifier
      val hadoopToken = new Token(identifier.getBytes, delegationToken.getPassword, identifier.getKind, delegationToken.getServiceName)
      jobContext.getConfiguration.asInstanceOf[JobConf].getCredentials.addToken(delegationToken.getServiceName, hadoopToken)
    }

    var rawSplits = inputFormat.getSplits(jobContext).toArray

    if(splitRanges) {
      rawSplits = rawSplits.flatMap(split=>{
        val batchSplit = split.asInstanceOf[BatchInputSplit]
        val maxRangesPerSplit = 6
        if(batchSplit.getRanges.size()>maxRangesPerSplit){
          val numberOfSubRanges = batchSplit.getRanges.size()/maxRangesPerSplit
          val elementsPerRange = batchSplit.getRanges.size()/numberOfSubRanges
          val ranges = new java.util.ArrayList(batchSplit.getRanges)

          val subLists = (0 until numberOfSubRanges).map(rangeIndex => {
            ranges.subList(elementsPerRange*rangeIndex,Math.min(ranges.size(),elementsPerRange*(rangeIndex+1)))
          })

          subLists.map(new BatchInputSplit(batchSplit.getTable,batchSplit.getTableId,_,batchSplit.getLocations))
            .map(split=>{
              split.setInstanceName(batchSplit.getInstanceName)
              split.setZooKeepers(batchSplit.getZooKeepers)
              split.setMockInstance(false)
              split.setPrincipal(batchSplit.getPrincipal)
              split.setToken(batchSplit.getToken)
              split.setAuths(batchSplit.getAuths)
              split.setFetchedColumns(batchSplit.getFetchedColumns)
              split.setIterators(batchSplit.getIterators)
              split.setLogLevel(batchSplit.getLogLevel)
              split
            })
        }else{
          Seq(batchSplit)
        }

      })

    }

    val orderedSplits = rawSplits.sortBy(f => f.asInstanceOf[BatchInputSplit].getRanges.iterator.next())
    val result = new Array[Partition](orderedSplits.size)
    for (i <- 0 until orderedSplits.size) {
      result(i) = new NewHadoopPartition(id, i, orderedSplits(i).asInstanceOf[InputSplit with Writable])
    }
    result
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(Key, Value)] = {
    val iter = new Iterator[(Key, Value)] {
      val split = theSplit.asInstanceOf[NewHadoopPartition]
      logDebug("Input split: " + split.serializableHadoopSplit)
      val conf = getConf

      val inputMetrics = context.taskMetrics().inputMetrics
      val existingBytesRead = inputMetrics.bytesRead


      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val getBytesReadCallback: Option[() => Long] = None


      val format = new AccumuloInputFormat()

      val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
      private var reader = format.createRecordReader(
        split.serializableHadoopSplit.value, hadoopAttemptContext)
      reader.initialize(split.serializableHadoopSplit.value, hadoopAttemptContext)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(context => close())
      var havePair = false
      var finished = false
      var recordsSinceMetricsUpdate = 0

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          if (finished) {
            // Close and release the reader here; close() will also be called when the task
            // completes, but for tasks that read from many files, it helps to release the
            // resources early.
            close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): (Key, Value) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false


        (reader.getCurrentKey, reader.getCurrentValue)
      }

      private def close() {
        if (reader != null) {
          // Close the reader and release it. Note: it's very important that we don't close the
          // reader more than once, since that exposes us to MAPREDUCE-5918 when running against
          // Hadoop 1.x and older Hadoop 2.x releases. That bug can lead to non-deterministic
          // corruption issues when reading compressed input.
          try {
            reader.close()
          } catch {
            case e: Exception =>
              logWarning("Exception in RecordReader.close()", e)
          } finally {
            reader = null
          }

        }
      }
    }
    new InterruptibleIterator(context, iter)
  }


  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val split = hsplit.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value
    val locs =
        try {
          val infos: Array[SplitLocationInfo] = split.getLocationInfo()
          convertSplitLocationInfo(infos)
        } catch {
          case e : Exception =>
            logDebug("Failed to use InputSplit#getLocationInfo.", e)
            None
        }

    locs.getOrElse(split.getLocations.filter(_ != "localhost"))
  }

  private def convertSplitLocationInfo(infos: Array[SplitLocationInfo]): Option[Seq[String]] = {
    Option(infos).map(_.flatMap { loc =>
      val locationStr = loc.getLocation()
      if (locationStr != "localhost") {
        if (loc.isInMemory) {
          logDebug(s"Partition $locationStr is cached by Hadoop.")
          Some("hdfs_cache_"+locationStr)
        } else {
          Some(locationStr)
        }
      } else {
        None
      }
    })
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
        " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

}
/*
private[spark] object NewHadoopRDD {
  /**
   * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
   * Therefore, we synchronize on this lock before calling new Configuration().
   */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()


}
*/