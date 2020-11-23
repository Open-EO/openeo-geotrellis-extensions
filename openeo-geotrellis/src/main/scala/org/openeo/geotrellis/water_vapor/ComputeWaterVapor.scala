package org.openeo.geotrellis.water_vapor

import java.util.concurrent.Callable

import com.google.common.cache.{Cache, CacheBuilder}
import geotrellis.layer._
import geotrellis.raster.{FloatConstantNoDataCellType, FloatConstantTile, MultibandTile, NODATA, Tile}
import geotrellis.spark._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory
import org.openeo.geotrellis.icor.LookupTable
import org.openeo.geotrellis.icor.LookupTableIO
import org.openeo.geotrellis.icor.DEMProvider
import org.openeo.geotrellis.icor.CorrectionDescriptorSentinel2

object ComputeWaterVapor{
  implicit val logger = LoggerFactory.getLogger(classOf[ComputeWaterVapor])
  val lutCache: Cache[String, Broadcast[LookupTable]] = CacheBuilder.newBuilder().softValues().build()
}


class ComputeWaterVapor {

  import ComputeWaterVapor._

  def correct(
        jsc: JavaSparkContext, 
        datacube: MultibandTileLayerRDD[SpaceTimeKey], 
        tableId: String, 
        bandIds:java.util.List[String], // 
        prePostMult:java.util.List[Double], // [1.e-4,1.]
        defParams:java.util.List[Double] // [ sza, saa, vza, vaa, aot (fixed override)=0.1, ozone (fixed override)=0.33 ]
      ): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {

    val sc = JavaSparkContext.toSparkContext(jsc)

    val lutLoader = new Callable[Broadcast[LookupTable]]() {

      override def call(): Broadcast[LookupTable] = {
        org.openeo.geotrellis.logTiming("Loading icor LUT")({
          sc.broadcast(LookupTableIO.readLUT(tableId))
        })
      }
    }

    val bcLUT = lutCache.get(tableId, lutLoader)

    val crs = datacube.metadata.crs
    val layoutDefinition = datacube.metadata.layout

    val auxDataAccum = sc.longAccumulator("Icor water vapor data loading")
    val correctionAccum = sc.longAccumulator("Icor water vapor calculator")

    new ContextRDD(
      datacube.mapPartitions(partition => {
// AOT overriden        val aotProvider = new AOTProvider()
        val demProvider = new DEMProvider(layoutDefinition, crs)
        
        partition.map {
          multibandtile =>
            (
              multibandtile._1,
              //          multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
              {
                val wvBandId="B09"
                val r0BandId="B8A"
                val r1BandId="B11"
                
                val cd = new CorrectionDescriptorSentinel2()
                val wvCalc = new WaterVaporCalculator()
                wvCalc.prepare(bcLUT.value,cd,wvBandId,r0BandId,r1BandId)

                def angleTile(index: Int, fallback: Double): Tile = {
                  if (index > 0) multibandtile._2.band(index).convert(FloatConstantNoDataCellType) else FloatConstantTile(fallback.toFloat, multibandtile._2.cols, multibandtile._2.rows)
                }
                
                val szaIdx = bandIds.indexOf("sunZenithAngles")
                val saaIdx = bandIds.indexOf("sunAzimuthAngles")
                val vzaIdx = bandIds.indexOf("viewZenithMean")
                val vaaIdx = bandIds.indexOf("viewAzimuthMean")

                val szaTile = angleTile(szaIdx, defParams.get(0))
                val saaTile = angleTile(saaIdx, defParams.get(1))// 130.0) // TODO: why is the fallback hardcoded to this value?
                val vzaTile = angleTile(vzaIdx, defParams.get(2))
                val vaaTile = angleTile(vaaIdx, defParams.get(3))// 0.0)   // TODO: why is the fallback hardcoded to this value?

                val raaTileDiff = saaTile - vaaTile
                val raaTile=raaTileDiff.mapDouble(v =>
                  ( if (v < -180.0) v+360.0 else if ( v > 180.0) v-360.0 else v ).abs  
                )

                val startMillis = System.currentTimeMillis();
// AOT overriden                val aotTile = aotProvider.computeAOT(multibandtile._1, crs, layoutDefinition)
                val aot=defParams.get(4)
                val ozone=defParams.get(5)
                val demTile = demProvider.computeDEM(multibandtile._1, crs, layoutDefinition)
                val afterAuxData = System.currentTimeMillis()
                
                // TODO: use reflToRad(double src, double sza, ZonedDateTime time, int bandToConvert)
                val wvTile= multibandtile._2.band(wvCalc.findIndexOf(bandIds,wvBandId))
                val r0Tile= multibandtile._2.band(wvCalc.findIndexOf(bandIds,r0BandId))
                val r1Tile= multibandtile._2.band(wvCalc.findIndexOf(bandIds,r1BandId))
                
                auxDataAccum.add(afterAuxData-startMillis)

                val mbtresult : MultibandTile = try {
                  val result : Tile = MultibandTile(
                      szaTile, 
                      vzaTile, 
                      raaTile,
                      demTile.convert(FloatConstantNoDataCellType), 
// AOT overriden                      aotTile.convert(FloatConstantNoDataCellType), 
                      wvTile.convert(FloatConstantNoDataCellType), 
                      r0Tile.convert(FloatConstantNoDataCellType), 
                      r1Tile.convert(FloatConstantNoDataCellType)
                      ).combineDouble(0, 1, 2, 3, 4, 5, 6 /*AOT overriden , 7*/) { 
                        (sza, vza, raa, dem, /*AOT overriden aot,*/ wv, r0, r1) => 
                          if ((wv != NODATA)&&(r0 != NODATA)&&(r1 != NODATA)) (prePostMult.get(1) * 
                              wvCalc.computePixel(bcLUT.value, sza, vza, raa, dem, aot, wv*prePostMult.get(0), r0*prePostMult.get(0), r1*prePostMult.get(0), ozone, NODATA)
                          ) 
                          else NODATA 
                      }
//                  result.convert(wvTile.cellType)
                  MultibandTile(result)
                } catch {
                  case e: IllegalArgumentException => MultibandTile(wvTile)
                }

                correctionAccum.add(System.currentTimeMillis() - afterAuxData)
                mbtresult
              }
            )
        }

      }),
      datacube.metadata
    )

  }
 
}


