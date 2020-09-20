package org.openeo.geotrellis.icor

import geotrellis.layer._
import geotrellis.spark._
import geotrellis.raster.MultibandTile

class AtmosphericCorrection {

  def printapply(){
      println("Hello, world from my scala thinggy!")
  }
  
  def correct(datacube: MultibandTileLayerRDD[SpaceTimeKey]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
      println("Hello, world from inside scala correct!")
      val icor=new AtmosphericCorrectionICOR()
      new ContextRDD(
          datacube.map(multibandtile => (
              multibandtile._1,
              multibandtile._2.mapBands((b, tile) => icor.correct(b,tile))
          )),
          datacube.metadata
      )
  }
   
//  def correct(datacube: MultibandTileLayerRDD[SpaceTimeKey]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
////    val icor=new AtmosphericCorrectionICOR()
//    new ContextRDD(
//      datacube.map(multibandtile => (
//        multibandtile._1,
//        multibandtile._2.mapBands((b, tile) => tile.map(i => 11))
//      )),
//      datacube.metadata
//    )
//  }
 
}


