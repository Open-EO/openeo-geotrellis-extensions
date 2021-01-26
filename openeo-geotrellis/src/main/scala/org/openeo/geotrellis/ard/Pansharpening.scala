package org.openeo.geotrellis.ard

import java.util

import geotrellis.layer.{SpaceTimeKey, SpatialKey}
import geotrellis.spark._

import scala.collection.JavaConverters._

/**
 * Pansharpen process
 */
class Pansharpening {

  def pansharpen_sentinel2( datacube: MultibandTileLayerRDD[SpaceTimeKey],band_index_10m: util.List[Int],band_index_20m: util.List[Int] ): MultibandTileLayerRDD[SpaceTimeKey]  = {
    return datacube.withContext(_.mapValues( tile => Improphe.improphe(null,tile,band_index_10m.asScala.toArray,band_index_20m.asScala.toArray,3,5)))

  }

  def pansharpen_sentinel2_spatial( datacube: MultibandTileLayerRDD[SpatialKey],band_index_10m: util.List[Int],band_index_20m: util.List[Int] ): MultibandTileLayerRDD[SpatialKey]  = {
    return datacube.withContext(_.mapValues( tile => Improphe.improphe(null,tile,band_index_10m.asScala.toArray,band_index_20m.asScala.toArray,3,5)))
  }

}
