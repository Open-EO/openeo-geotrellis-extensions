package org.openeo.geotrellisaccumulo

import geotrellis.layer.{Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.KryoWrapper
import geotrellis.store.avro.codecs.KeyValueRecordCodec
import geotrellis.store.avro.{AvroEncoder, AvroRecordCodec}
import geotrellis.store.index.KeyIndex
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

class GeotrellisRasterRDD[V : AvroRecordCodec: ClassTag](keyIndex:KeyIndex[SpaceTimeKey],writerSchema:Schema,parent:GeotrellisAccumuloRDD,val metadata: TileLayerMetadata[SpaceTimeKey], sc : SparkContext) extends RDD[(SpaceTimeKey, V)](sc,Nil) with Metadata[TileLayerMetadata[SpaceTimeKey]] {

  val codec = KryoWrapper(KeyValueRecordCodec[SpaceTimeKey, V])
  val kwWriterSchema = KryoWrapper(Some(writerSchema))

  override val partitioner: Option[org.apache.spark.Partitioner] = Some(SpacePartitioner(metadata.bounds))

  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeKey, V)] = {
    val parentIterator = parent.compute(split, context)

    parentIterator.map { case (_, value) =>
      AvroEncoder.fromBinary(kwWriterSchema.value.getOrElse(codec.value.schema), value.get)(codec.value)
    }.flatten
  }

  override protected def getPartitions: Array[Partition] = parent.getPartitions
}
