package org.openeo.geotrellis

import java.time.{Instant, ZonedDateTime}

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal.{Convolve, Kernel, TargetCell}
import geotrellis.raster.mapalgebra.local._
import geotrellis.spark.{ContextRDD, Metadata, MultibandTileLayerRDD, SpaceTimeKey, SpatialComponent, SpatialKey, TemporalKey, TileLayerMetadata}
import org.apache.spark.rdd.RDD
import org.openeo.geotrellis.focal._

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.reflect.ClassTag


class OpenEOProcesses extends Serializable {

  val unaryProcesses: Map[String, Tile => Tile] = Map(
    "absolute" -> Abs.apply,
    //TODO "exp"
    "ln" -> Log.apply,
    //TODO "log"
    "sqrt" -> Sqrt.apply,
    "ceil" -> Ceil.apply,
    "floor" -> Floor.apply,
    //TODO: "int" integer part of a number
    "round" -> Round.apply,
    "arccos" -> Acos.apply,
    //TODO "arccos" -> Acosh.apply,
    "arcsin" -> Asin.apply,
    "arctan" -> Atan.apply,
    //TODO: arctan 2 is not unary! "arctan2" -> Atan2.apply,
    "cos" -> Cos.apply,
    "cosh" -> Cosh.apply,
    "sin" -> Sin.apply,
    "sinh" -> Sinh.apply,
    "tan" -> Tan.apply,
    "tanh" -> Tanh.apply
  )

  def applyProcess[K](datacube:MultibandTileLayerRDD[K], process:String): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]= {
    return ContextRDD(datacube.map(multibandtile => (multibandtile._1,multibandtile._2.mapBands((b,t) => unaryProcesses.get(process).get(t) ))),datacube.metadata)
  }

  def mapInstantToInterval(datacube:MultibandTileLayerRDD[SpaceTimeKey], intervals:java.lang.Iterable[String],labels:java.lang.Iterable[String]) :MultibandTileLayerRDD[SpaceTimeKey] = {
    val timePeriods: Seq[Iterable[Instant]] = JavaConverters.iterableAsScalaIterableConverter(intervals).asScala.map(s => Instant.parse(s)).grouped(2).toList
    val periodsToLabels: Seq[(Iterable[Instant], String)] = timePeriods.zip(labels.asScala)
    val tilesByInterval: RDD[(SpaceTimeKey, MultibandTile)] = datacube.flatMap(tuple => {
      val instant = tuple._1.time.toInstant
      val spatialKey = tuple._1.spatialKey
      val labelsForKey = periodsToLabels.filter(p => {
        val interval = p._1
        val iterator = interval.toIterator
        val leftBound = iterator.next()
        val rightBound = iterator.next()
        (leftBound.isBefore(instant) && rightBound.isAfter(instant)) || leftBound.equals(instant)
      }).map(t => t._2).map(ZonedDateTime.parse(_))

      labelsForKey.map(l => (SpaceTimeKey(spatialKey,TemporalKey(l)),tuple._2))
    })
    return ContextRDD(tilesByInterval, datacube.metadata)

  }

  def mapBands[K](datacube:MultibandTileLayerRDD[K], scriptBuilder:OpenEOProcessScriptBuilder): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]={
    val function = scriptBuilder.generateFunction()
    return ContextRDD[K,MultibandTile,TileLayerMetadata[K]](datacube.map(tile => {
      val resultTiles = function(tile._2.bands)
      (tile._1,MultibandTile(resultTiles))
    }),datacube.metadata)
  }


  def rasterMask(datacube:MultibandTileLayerRDD[SpaceTimeKey],mask:MultibandTileLayerRDD[SpaceTimeKey], replacement:Double):ContextRDD[SpaceTimeKey,MultibandTile,TileLayerMetadata[SpaceTimeKey]] = {
    val joined = datacube.spatialLeftOuterJoin(mask)
    val replacementInt: Int = replacement.intValue()
    val replacementDouble: Double = replacement
    val masked = joined.mapValues(t => {
      val dataTile = t._1
      if (!t._2.isEmpty) {
        val maskTile = t._2.get
        var maskIndex = 0
        dataTile.mapBands((index,tile) =>{
          if(dataTile.bandCount == maskTile.bandCount){
            maskIndex = index
          }
          tile.dualCombine(maskTile.band(maskIndex))((v1,v2) => if (v2 > 0 && isData(v1)) replacementInt else v1)((v1,v2) => if (v2 > 0.0 && isData(v1)) replacementDouble else v1)
        })

      } else {
        dataTile
      }

    })

    return new ContextRDD(masked,datacube.metadata)
  }

  /**
    * Implementation of openeo apply_kernel
    * https://open-eo.github.io/openeo-api/v/0.4.2/processreference/#apply_kernel
    *
    * @param datacube
    * @param kernel The kernel to be applied on the data cube. The kernel has to be as many dimensions as the data cube has dimensions.
    *
    *               This is basically a shortcut for explicitly multiplying each value by a factor afterwards, which is often required for some kernel-based algorithms such as the Gaussian blur.
    * @tparam K
    * @return
    */
  def apply_kernel[K: SpatialComponent: ClassTag](datacube:MultibandTileLayerRDD[K],kernel:Tile): RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]] = {
    val k = new Kernel(kernel)
    return MultibandFocalOperation(datacube, k, None){ (tile, bounds) => Convolve(tile, k, bounds, TargetCell.All) }
  }

  /**
    * Apply kernel for spacetime data cubes.
    * @see #apply_kernel
    *
    */
  def apply_kernel_spacetime(datacube:MultibandTileLayerRDD[SpaceTimeKey],kernel:Tile): RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = {
    return apply_kernel(datacube,kernel)
  }

  /**
    * Apply kernel for spatial data cubes.
    * @see #apply_kernel
    */
  def apply_kernel_spatial(datacube:MultibandTileLayerRDD[SpatialKey], kernel:Tile): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] = {
    return apply_kernel(datacube,kernel)
  }

}
