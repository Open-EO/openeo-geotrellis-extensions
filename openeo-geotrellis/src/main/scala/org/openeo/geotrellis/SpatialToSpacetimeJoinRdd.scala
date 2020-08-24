package org.openeo.geotrellis

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.MultibandTile
import geotrellis.spark.{MultibandTileLayerRDD, _}
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.store.index.zcurve.Z3
import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect._


class SpatialToSpacetimeJoinRdd[T : ClassTag](spacetimeRDD: MultibandTileLayerRDD[SpaceTimeKey], spatialRdd: RDD[(SpatialKey,T)]) extends RDD[(SpaceTimeKey, (MultibandTile,T))](spacetimeRDD.context,Nil) {

  val spatiallyPartitionedRdd: MultibandTileLayerRDD[SpaceTimeKey] =  {
    if(spacetimeRDD.partitioner.isEmpty || !spacetimeRDD.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]]){
      spacetimeRDD.withContext{_.partitionBy( SpacePartitioner(spacetimeRDD.metadata.bounds.get)) }
    }else{
      spacetimeRDD
    }
  }
  val spacePartitioner:SpacePartitioner[SpaceTimeKey] = spatiallyPartitionedRdd.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]]


  val someDate = spatiallyPartitionedRdd.metadata.bounds.get._1.time
  val spatialRDDAsSpacetime = new ShuffledRDD[SpaceTimeKey, T, T](spatialRdd.map(spatialkey_tile => {
    (SpaceTimeKey(spatialkey_tile._1, someDate), spatialkey_tile._2)
  }),spacePartitioner)


  override val partitioner: Option[Partitioner] = Some(spacePartitioner)

  class SpatialDependency() extends NarrowDependency[(SpaceTimeKey,T)](spatialRDDAsSpacetime) {

    /**
     * This is a bit fragile: this decoding belongs to a very specific SpaceTimePartitioner index
     * Using any other index will fail!
     * @param region
     * @return
     */
    def decodeIndexKey(region:BigInt):SpaceTimeKey = {
      val (x,y,t) = new Z3(region.longValue() << 8 ).decode
      new SpaceTimeKey(x,y,t*1000L * 60 * 60 * 24 )
    }

    override def getParents(partitionId: Int): List[Int] = {
      val theRegion = spacePartitioner.regions(partitionId)
      val keyForPartition = decodeIndexKey(theRegion)
      val matchingPartition = spacePartitioner.getPartition(SpaceTimeKey(keyForPartition.spatialKey, someDate))
      List(matchingPartition)
    }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    Seq(new OneToOneDependency(spatiallyPartitionedRdd), new SpatialDependency())
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeKey, (MultibandTile,T))] = {

    val originalIterator = spatiallyPartitionedRdd.compute(split, context).toSeq
    if(originalIterator.isEmpty) {
      return Iterator.empty
    }
    val spatialPartition = spacePartitioner.getPartition(SpaceTimeKey(originalIterator.head._1.spatialKey,someDate))

    val theMatchingSpatialPartition = spatialRDDAsSpacetime.partitions(spatialPartition)
    val spatialIterator = spatialRDDAsSpacetime.compute(theMatchingSpatialPartition, context)
    val spatialMap = spatialIterator.toMap
    originalIterator.filter(t => spatialMap.contains(SpaceTimeKey(t._1.spatialKey,someDate))).map(tuple => (tuple._1,(tuple._2,spatialMap(SpaceTimeKey(tuple._1.spatialKey,someDate))))).iterator

  }

  override protected def getPartitions: Array[Partition] ={spatiallyPartitionedRdd.partitions}
}
