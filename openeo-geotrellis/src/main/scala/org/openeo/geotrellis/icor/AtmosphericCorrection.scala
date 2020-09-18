package org.openeo.geotrellis.icor

import geotrellis.layer.SpaceTimeKey
import geotrellis.spark.{MultibandTileLayerRDD, _}

class AtmosphericCorrection {

  def printapply(){
      println("Hello, world from my scala thinggy!")
  }
  
  def correct(datacube: MultibandTileLayerRDD[SpaceTimeKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
    return ContextRDD(
      datacube,
      datacube.metadata
    )
  }  
   
 
}

//    val proc = unaryProcesses(process)
//    return ContextRDD(
//      datacube.map(multibandtile => (
//        multibandtile._1,
//        multibandtile._2.mapBands((b, tile) => proc(tile))
//      )),
//      datacube.metadata
//    )


//  def (datacube: MultibandTileLayerRDD[SpaceTimeKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
//  def (datacube: MultibandTileLayerRDD[SpaceTimeKey]): MultibandTileLayerRDD[SpaceTimeKey] = {
//  def (datacube: MultibandTileLayerRDD[SpaceTimeKey]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = {
//    val joined = datacube.spatialLeftOuterJoin(mask)
//    val replacementInt: Int = if (replacement == null) NODATA else replacement.intValue()
//    val replacementDouble: Double = if (replacement == null) doubleNODATA else replacement
//    val masked = joined.mapValues(t => {
//      val dataTile = t._1
//      if (!t._2.isEmpty) {
//        val maskTile = t._2.get
//        var maskIndex = 0
//        dataTile.mapBands((index,tile) =>{
//          if(dataTile.bandCount == maskTile.bandCount){
//            maskIndex = index
//          }
//          tile.dualCombine(maskTile.band(maskIndex))((v1,v2) => if (v2 != 0 && isData(v1)) replacementInt else v1)((v1,v2) => if (v2 != 0.0 && isData(v1)) replacementDouble else v1)
//        })
//
//      } else {
//        dataTile
//      }
//
//    })
//
//    new ContextRDD(masked, datacube.metadata)
//  }
