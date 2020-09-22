package org.openeo.geotrellis.icor

import org.apache.spark.api.java.JavaSparkContext
//import org.apache.spark.SparkContext

import geotrellis.layer._
import geotrellis.spark._
import geotrellis.raster.MultibandTile




class AtmosphericCorrection {

  def printapply(){
      println("Hello, world from my scala thinggy!")
  }
  
     
  def correct(jsc: JavaSparkContext, datacube: MultibandTileLayerRDD[SpaceTimeKey], tableId: String): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
    // TODO: mechanism to map bands properly
    val sc = JavaSparkContext.toSparkContext(jsc)
    val lut=new LookupTable(tableId)
    val bcLUT = sc.broadcast(lut)
    new ContextRDD(
      datacube.map(multibandtile => (
        multibandtile._1,
//        multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
        multibandtile._2.mapBands((b, tile) => {
          val cd=new CorrectionDescriptor()
          tile.map(i => cd.correct(bcLUT.value, b, i.toDouble, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0).toInt) 
        })
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

