package org.openeo.geotrellis.icor

import org.apache.spark.api.java.JavaSparkContext
import geotrellis.layer._
import geotrellis.spark._
import geotrellis.raster.{MultibandTile,NODATA}
import java.util.ArrayList




class AtmosphericCorrection {

  def printapply(){
      println("Hello, world from my scala thinggy!")
  }
  
     
  def correct(
        jsc: JavaSparkContext, 
        datacube: MultibandTileLayerRDD[SpaceTimeKey], 
        tableId: String, 
        bandIds:ArrayList[String],
        prePostMult:ArrayList[Double],
        defParams:ArrayList[Double]
      ): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
    
    // TODO: get bandids by name
    
//    var retRDD: ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]] = new ContextRDD(datacube,datacube.metadata)
//    try {

      val sc = JavaSparkContext.toSparkContext(jsc)
      val lut=LookupTableIO.readLUT(tableId)
      val bcLUT = sc.broadcast(lut)
//      print("**** NODAT: "+NODATA.getClass().toString()+":"+NODATA)
      new ContextRDD(
        datacube.map(multibandtile => (
          multibandtile._1,
//          multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
          multibandtile._2.mapBands((b, tile) => {
            val cd=new CorrectionDescriptorSentinel2()
            val iband=cd.getBandFromName(bandIds.get(b))
            tile.map(i => if (i!=NODATA) (prePostMult.get(1)*cd.correct(bcLUT.value, iband, multibandtile._1.instant, i.toDouble*prePostMult.get(0), defParams.get(0), defParams.get(1), defParams.get(2), defParams.get(3), defParams.get(4), defParams.get(5), defParams.get(6), 0)).toInt else NODATA ) 
//            tile.map(i => cd.correct(bcLUT.value, b, i.toDouble, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0).toInt ) 
          })
        )),
        datacube.metadata
      )

//      println("!!!!!!!!!!!!! "+retRDD.getClass().toString())
//      retRDD
//    } catch {
//      case e: Exception => { 
//        println("***********************************\n"+e+"\n"+e.getStackTrace+"\n")
//        throw e
//      }
//    }
      
      
  }
 
}


