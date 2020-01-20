package org.openeo.geotrellisaccumulo

import geotrellis.layer.{Metadata, SpaceTimeKey, TileLayerMetadata}
import geotrellis.spark.util.KryoWrapper
import geotrellis.store.accumulo.AccumuloKeyEncoder
import geotrellis.store.avro.codecs.KeyValueRecordCodec
import geotrellis.store.avro.{AvroEncoder, AvroRecordCodec}
import geotrellis.store.index.KeyIndex
import org.apache.accumulo.core.client.mapreduce.impl.BatchInputSplit
import org.apache.accumulo.core.data.Key
import org.apache.avro.Schema
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, RangePartitioner, SparkContext, TaskContext}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class GeotrellisRasterRDD[V : AvroRecordCodec: ClassTag](keyIndex:KeyIndex[SpaceTimeKey],writerSchema:Schema,parent:GeotrellisAccumuloRDD,val metadata: TileLayerMetadata[SpaceTimeKey], sc : SparkContext) extends RDD[(SpaceTimeKey, V)](sc,Nil) with Metadata[TileLayerMetadata[SpaceTimeKey]] {

  val codec = KryoWrapper(KeyValueRecordCodec[SpaceTimeKey, V])
  val kwWriterSchema = KryoWrapper(Some(writerSchema))

  override val partitioner: Option[org.apache.spark.Partitioner] = Some(new RangePartitioner[SpaceTimeKey,V](partitions.length,SparkContext.getOrCreate().emptyRDD[(SpaceTimeKey, V)]){
    override def numPartitions: Int = {
      partitions.length
    }

    override def getPartition(key: Any): Int = {
      println(key)
      val accumuloKey = new Key(new Text(AccumuloKeyEncoder.long2Bytes(keyIndex.toIndex(key.asInstanceOf[SpaceTimeKey]))))
      println(accumuloKey)
      val index = partitions.indexWhere(p => p.asInstanceOf[NewHadoopPartition].serializableHadoopSplit.value.asInstanceOf[BatchInputSplit].getRanges().asScala.exists( r => r.contains(accumuloKey)) )
      if(index < 0) {
        //cannot find this key in the partition, avoid breaking spark by returning -1
        return 0
      }
      return index
    }
  })

  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeKey, V)] = {
    val parentIterator = parent.compute(split, context)

    return parentIterator.map{ case (_, value) =>
      AvroEncoder.fromBinary(kwWriterSchema.value.getOrElse(codec.value.schema), value.get)(codec.value)
    }.flatMap { pairs: Vector[(SpaceTimeKey, V)] =>
      pairs
    }
  }

  override protected def getPartitions: Array[Partition] = {
    return parent.getPartitions
  }
}
