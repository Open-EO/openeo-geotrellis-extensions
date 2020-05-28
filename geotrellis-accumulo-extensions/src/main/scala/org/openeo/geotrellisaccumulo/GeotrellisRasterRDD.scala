package org.openeo.geotrellisaccumulo

import java.time.format.DateTimeFormatter
import java.util

import geotrellis.layer.{Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.KryoWrapper
import geotrellis.store.accumulo.AccumuloKeyEncoder
import geotrellis.store.avro.codecs.KeyValueRecordCodec
import geotrellis.store.avro.{AvroEncoder, AvroRecordCodec}
import geotrellis.store.index.KeyIndex
import org.apache.accumulo.core.client.mapreduce.impl.BatchInputSplit
import org.apache.accumulo.core.data
import org.apache.accumulo.core.data.Key
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.openeo.geotrellisaccumulo

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.Breaks._

class GeotrellisRasterRDD[V : AvroRecordCodec: ClassTag](keyIndex:KeyIndex[SpaceTimeKey],writerSchema:Schema,parent:GeotrellisAccumuloRDD,val metadata: TileLayerMetadata[SpaceTimeKey], sc : SparkContext) extends RDD[(SpaceTimeKey, V)](sc,Nil) with Metadata[TileLayerMetadata[SpaceTimeKey]] {

  val codec = KryoWrapper(KeyValueRecordCodec[SpaceTimeKey, V])
  val kwWriterSchema = KryoWrapper(Some(writerSchema))

  override val partitioner: Option[org.apache.spark.Partitioner] = Some(SpacePartitioner(metadata.bounds))

  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeKey, V)] = {
    if(!split.isInstanceOf[NewHadoopPartition]) {
      return Iterator.empty
    }
    val parentIterator = parent.compute(split, context)

    parentIterator.map { case (_, value) =>
      AvroEncoder.fromBinary(kwWriterSchema.value.getOrElse(codec.value.schema), value.get)(codec.value)
    }.flatten
  }

  class EmptyPartition(theIndex:Int) extends Partition{
    override def index: Int = theIndex
  }

  override protected def getPartitions: Array[Partition] = {
    val myPartitions: Array[Partition] = parent.getPartitions
    val myRegions = partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]].regions

    var splitsForRegions = mutable.Seq[BatchInputSplit]()
    var currentStart = 0
    println("Required input data is computed, this can take a while!")
    val start = System.currentTimeMillis()
    for (region <- myRegions) {
      val startKey = geotrellisaccumulo.decodeIndexKey(region)
      val endKey = geotrellisaccumulo.decodeIndexKey(region + 1)
      val start = new Key(AccumuloKeyEncoder.index2RowId(keyIndex.toIndex(startKey)))
      val end = new Key(AccumuloKeyEncoder.index2RowId(keyIndex.toIndex(endKey)))
      // assert(region+1==geotrellisaccumulo.SpaceTimeByMonthPartitioner.toIndex(endKey))
      // println("Region: " + region + " date: " + DateTimeFormatter.BASIC_ISO_DATE.format(startKey.time) + " enddate: " + DateTimeFormatter.BASIC_ISO_DATE.format(endKey.time))

      var newSplit: BatchInputSplit = null
      breakable {
        for (partition <- myPartitions.drop(currentStart)) {

          var rangesForRegion = mutable.Seq[data.Range]()
          val inputSplit = partition.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value.asInstanceOf[BatchInputSplit]

          var indices = ""
          var rangeIdx = 0
          var partitionOutOfCurrentBounds = false

          for (range <- inputSplit.getRanges().toArray) {
            val theRange = range.asInstanceOf[data.Range]
            if (!theRange.beforeStartKey(end) && (!theRange.afterEndKey(start) || theRange.getEndKey == start)) {
              val clippedRange = theRange.clip(new data.Range(start, true, end, false), true)
              if (clippedRange != null) {
                indices += (rangeIdx + ", ")
                rangesForRegion = rangesForRegion :+ clippedRange
              } else {
                // println("No overlap!!")
              }

            } else if (theRange.beforeStartKey(end)) {
              //the end of the region lies before the start key of the range, so this partition is entirely out of bounds
              partitionOutOfCurrentBounds = true
            }
            rangeIdx += 1
          }
          if (indices.length > 0) {
            //println("Partition: " + partition.index + " idx: " + indices)
            currentStart = partition.index
            if (newSplit == null) {
              newSplit = new BatchInputSplit(inputSplit.getTable, inputSplit.getTableId, new util.ArrayList(rangesForRegion.asJavaCollection), inputSplit.getLocations)
              newSplit.setInstanceName(inputSplit.getInstanceName)
              newSplit.setZooKeepers(inputSplit.getZooKeepers)
              newSplit.setMockInstance(false)
              newSplit.setPrincipal(inputSplit.getPrincipal)
              newSplit.setToken(inputSplit.getToken)
              newSplit.setAuths(inputSplit.getAuths)
              newSplit.setFetchedColumns(inputSplit.getFetchedColumns)
              newSplit.setIterators(inputSplit.getIterators)
              newSplit.setLogLevel(inputSplit.getLogLevel)
            } else {
              newSplit.getRanges.addAll(rangesForRegion.asJavaCollection)
            }
          }
          if (partitionOutOfCurrentBounds) {
            break
          }
        }


      }
      splitsForRegions = splitsForRegions :+ newSplit
    }
    println("Computed input data in: " + (System.currentTimeMillis()-start)/1000.0 + " seconds")
    //region indices map directly to partition indices!
    assert( myRegions.size == splitsForRegions.size)
    var i = -1
    splitsForRegions.seq.map(split => {
      i+=1
      if(split==null){
        new EmptyPartition(i)
      }else{
        new NewHadoopPartition(id, i, split)
      }
    }).toArray[Partition]
  }
}
