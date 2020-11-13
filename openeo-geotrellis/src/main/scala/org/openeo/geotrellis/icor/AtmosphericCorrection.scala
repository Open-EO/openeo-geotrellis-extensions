package org.openeo.geotrellis.icor

import geotrellis.layer._
import geotrellis.raster.{MultibandTile, NODATA, Tile}
import geotrellis.spark._
import org.apache.spark.api.java.JavaSparkContext




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

      val crs = datacube.metadata.crs
      val layoutDefinition = datacube.metadata.layout

      new ContextRDD(
        datacube.mapPartitions(partition => {
          val aotProvider = new AOTProvider()
          partition.map {
            multibandtile =>
              (
                multibandtile._1,
                //          multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
                multibandtile._2.mapBands((b, tile) => {
                  val cd = new CorrectionDescriptorSentinel2()
                  val iband = cd.getBandFromName(bandIds.get(b))

                  val aotTile = aotProvider.computeAOT(multibandtile._1,crs,layoutDefinition)
                  val resultTile:Tile = MultibandTile(tile,aotTile.convert(tile.cellType)).combine(0,1){(refl,aot) => if (refl != NODATA) (prePostMult.get(1) * cd.correct(bcLUT.value, iband, multibandtile._1.instant, refl.toDouble * prePostMult.get(0), defParams.get(0), defParams.get(1), defParams.get(2), defParams.get(3),aot, defParams.get(5),defParams.get(6),0)).toInt else NODATA}
                  resultTile
                })
              )
          }

        }),
        datacube.metadata
      )

  }
 
}


