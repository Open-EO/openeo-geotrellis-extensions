package org.openeo.geotrellis.icor

import org.apache.spark.api.java.JavaSparkContext
import geotrellis.layer._
import geotrellis.spark._
import geotrellis.raster.{MultibandTile,NODATA}




class AtmosphericCorrection {

  def printapply(){
      println("Hello, world from my scala thinggy!")
  }
  
     
  def correct(
        jsc: JavaSparkContext, 
        datacube: MultibandTileLayerRDD[SpaceTimeKey], 
        tableId: String, 
        bandIds:java.util.List[String],
        prePostMult:java.util.List[Double],
        defParams:java.util.List[Double]
      ): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
    
      val sc = JavaSparkContext.toSparkContext(jsc)
      val lut=LookupTableIO.readLUT(tableId)
      val bcLUT = sc.broadcast(lut)
      new ContextRDD(
        datacube.map(multibandtile => (
          multibandtile._1,
//          multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
          multibandtile._2.mapBands((b, tile) => {
            val cd=new CorrectionDescriptorSentinel2()
            val iband=cd.getBandFromName(bandIds.get(b))
            tile.map(i => if (i!=NODATA) (prePostMult.get(1)*cd.correct(bcLUT.value, iband, multibandtile._1.instant, i.toDouble*prePostMult.get(0), defParams.get(0), defParams.get(1), defParams.get(2), defParams.get(3), defParams.get(4), defParams.get(5), defParams.get(6), 0)).toInt else NODATA ) 
          })
        )),
        datacube.metadata
      )

  }
 
}


