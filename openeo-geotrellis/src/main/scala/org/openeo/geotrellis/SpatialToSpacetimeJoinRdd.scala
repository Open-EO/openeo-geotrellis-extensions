package org.openeo.geotrellis

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.raster.MultibandTile
import geotrellis.spark.partition.{PartitionerIndex, SpacePartitioner}
import geotrellis.spark.{MultibandTileLayerRDD, _}
import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.locationtech.sfcurve.zorder.Z3
import org.openeo.geotrelliscommon.{ConfigurableSpaceTimePartitioner, DatacubeSupport, SpaceTimeByMonthPartitioner}

import scala.reflect._


class SpatialToSpacetimeJoinRdd[T : ClassTag](spacetimeRDD: MultibandTileLayerRDD[SpaceTimeKey], spatialRdd: RDD[(SpatialKey,T)], leftOuterJoin: Boolean = false) extends RDD[(SpaceTimeKey, (MultibandTile,Option[T]))](spacetimeRDD.context,Nil) {

  val index: Option[PartitionerIndex[SpaceTimeKey]] = DatacubeSupport.maybePartitionerIndex(spacetimeRDD)
  var indexReduction: Int = SpaceTimeByMonthPartitioner.DEFAULT_INDEX_REDUCTION
  val spatiallyPartitionedRdd: MultibandTileLayerRDD[SpaceTimeKey] =  {
    if(index.isEmpty || !index.get.isInstanceOf[ConfigurableSpaceTimePartitioner] ){
      // Note: SpacePartitioner is a case class whose equality (used by RDD.partitionBy to decide
      // whether a shuffle can be skipped) only considers `bounds`, not `index` (which lives in a
      // separate implicit parameter list). So a plain `rdd.partitionBy(newPartitioner)` would be a
      // no-op here when bounds happen to match, even though the index (and thus the physical
      // partitioning) actually differs. SpacePartitioner#apply explicitly compares indices via
      // `hasSameIndex` and forces a real shuffle (ShuffledRDD) when they differ.
      val newPartitioner = SpacePartitioner[SpaceTimeKey](spacetimeRDD.metadata.bounds.get)(SpaceTimeKey.Boundable, ClassTag(classOf[SpaceTimeKey]), new ConfigurableSpaceTimePartitioner)
      ContextRDD(newPartitioner(spacetimeRDD), spacetimeRDD.metadata)
    }else{

      indexReduction = index.get.asInstanceOf[ConfigurableSpaceTimePartitioner].indexReduction
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
      val (x,y,t) = new Z3(region.longValue << indexReduction ).decode
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

  override def compute(split: Partition, context: TaskContext): Iterator[(SpaceTimeKey, (MultibandTile,Option[T]))] = {

    val originalIterator = spatiallyPartitionedRdd.iterator(split, context).toSeq
    if(originalIterator.isEmpty) {
      return Iterator.empty
    }
    val spatialPartition = spacePartitioner.getPartition(SpaceTimeKey(originalIterator.head._1.spatialKey,someDate))

    val theMatchingSpatialPartition = spatialRDDAsSpacetime.partitions(spatialPartition)
    val spatialIterator = spatialRDDAsSpacetime.iterator(theMatchingSpatialPartition, context)
    val spatialMap = spatialIterator.toMap
    val joined = originalIterator.map(tuple => (tuple._1, (tuple._2, spatialMap.get(SpaceTimeKey(tuple._1.spatialKey,someDate)))))
    if(leftOuterJoin) {
      joined.iterator
    } else {
      joined.filter(_._2._2.isDefined).iterator
    }

  }

  override protected def getPartitions: Array[Partition] ={spatiallyPartitionedRdd.partitions}
}
