package org.openeo.geotrellis.icor

import geotrellis.layer._
import geotrellis.spark._
import geotrellis.raster.MultibandTile
//import org.apache.spark.SparkContext

// MAYBE THIS? import org.apache.spark.api.java.JavaSparkContext;


class AtmosphericCorrection {

  def printapply(){
      println("Hello, world from my scala thinggy!")
  }
     
  def correct(datacube: MultibandTileLayerRDD[SpaceTimeKey]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
    println("*** SCALA START *********************************************")
//    val bcICOR = SparkContext.getOrCreate().broadcast(123)
    new ContextRDD(
      datacube.map(multibandtile => (
        multibandtile._1,
        multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
      )),
      datacube.metadata
    )
  }
 
}

//  def correct(datacube: MultibandTileLayerRDD[SpaceTimeKey]): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
//      println("Hello, world from inside scala correct!")
//      val icor=new AtmosphericCorrectionICOR()
//      new ContextRDD(
//          datacube.map(multibandtile => (
//              multibandtile._1,
//              multibandtile._2.mapBands((b, tile) => icor.correct(b,tile))
//          )),
//          datacube.metadata
//      )
//  }


        //  println("*** SCALA MAP *********************************************")
          //println(bcICOR.getClass().toString())
          //println(bcICOR.value.getClass().toString())
          //bcICOR.value.correctPixel(i)
        //  14
        //}
//    val bcICOR = SparkContext.getOrCreate().broadcast(new AtmosphericCorrectionICOR())
//    println(bcICOR.getClass().toString())
//    println(bcICOR.value.getClass().toString())
//    println("*** SCALA ICOR DONE *********************************************")

