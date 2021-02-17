package org.openeo.geotrellis.icor

import java.util

import com.google.common.cache.{Cache, CacheBuilder}
import geotrellis.layer._
import geotrellis.raster.{FloatConstantNoDataCellType, FloatConstantTile, MultibandTile, NODATA, Tile}
import geotrellis.spark._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.openeo.geotrellis.water_vapor.{CWVProvider, ConstantCWVProvider}
import org.slf4j.LoggerFactory

object AtmosphericCorrection{
  implicit val logger = LoggerFactory.getLogger(classOf[AtmosphericCorrection])
  val iCorLookupTableCache: Cache[String, Broadcast[LookupTable]] = CacheBuilder.newBuilder().softValues().build()
}


class AtmosphericCorrection {

  def correct(
        jsc: JavaSparkContext,
        datacube: MultibandTileLayerRDD[SpaceTimeKey],
        bandIds:java.util.List[String],
        overrideParams:java.util.List[Double], // sza,vza,raa,gnd,aot,cwv,ozone <- if other than NaN, it will use the value as constant tile
        elevationSource: String,
        sensorId: String, // SENTINEL2 and LANDSAT8 for now
        // TODO: in the future SENTINEL2A,SENTINEL2B,... granulation will be needed
        appendDebugBands: Boolean // this will add sza,vza,raa,gnd,aot,cwv to the multiband tile result
      ): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
    this.correct("icor",null,jsc, datacube, bandIds, overrideParams, elevationSource, sensorId, appendDebugBands)
  }

    def correct(
                 method:String,
                 elevationModel:String,
                 jsc: JavaSparkContext,
                 datacube: MultibandTileLayerRDD[SpaceTimeKey],
                 bandIds:java.util.List[String],
                 overrideParams:java.util.List[Double], // sza,vza,raa,gnd,aot,cwv,ozone <- if other than NaN, it will use the value as constant tile
                 elevationSource: String,
                 sensorId: String, // SENTINEL2 and LANDSAT8 for now
                 // TODO: in the future SENTINEL2A,SENTINEL2B,... granulation will be needed
                 appendDebugBands: Boolean // this will add sza,vza,raa,gnd,aot,cwv to the multiband tile result
               ): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {
    val sc = JavaSparkContext.toSparkContext(jsc)

    val sensorDescriptor: CorrectionDescriptor = sensorId.toUpperCase() match {
      case "SENTINEL2"  => new Sentinel2Descriptor()
      case "LANDSAT8"   => new Landsat8Descriptor()
    }



    val crs = datacube.metadata.crs
    val layoutDefinition = datacube.metadata.layout

    val auxDataAccum = sc.longAccumulator("Icor aux data loading")
    val correctionAccum = sc.longAccumulator("Icor correction")
    
    new ContextRDD(
      datacube.mapPartitions(partition => {

        val aotProvider = new AOTProvider()

        val elevationProvider: ElevationProvider = elevationSource.toUpperCase() match {
          case "DEM"  => new DEMProvider(layoutDefinition, crs)
          case "SRTM" => new SRTMProvider()
        }

        // TODO: this is temporary, until water vapor calculator is refactored, remove  constant provider when not needed any more
        val cwvProvider = sensorId.toUpperCase() match {
          case "SENTINEL2"  => new CWVProvider(sensorDescriptor.asInstanceOf[ICorCorrectionDescriptor])
          case "LANDSAT8"   => new ConstantCWVProvider(0.0)
        }

        partition.map {
          multibandtile =>
            (
              multibandtile._1,
              //          multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
              {

                def angleTile(index: Int, fallback: Double): Tile = {
                  if (index > 0) multibandtile._2.band(index).convert(FloatConstantNoDataCellType) else FloatConstantTile(fallback.toFloat, multibandtile._2.cols, multibandtile._2.rows)
                }

                // Angles assignment priority order:
                // 1. overrideParam as constant tile if overrideParams not nan
                // 2. from band 
                // 3. fallback value
                // TODO: extra check when not all angles come from bands
                val szaIdx = if (overrideParams.get(0).isNaN) bandIds.indexOf("sunZenithAngles")  else -1
                val vzaIdx = if (overrideParams.get(1).isNaN) bandIds.indexOf("viewZenithMean")   else -1
                val saaIdx = if (overrideParams.get(2).isNaN) bandIds.indexOf("sunAzimuthAngles") else -1
                val vaaIdx = if (overrideParams.get(2).isNaN) bandIds.indexOf("viewAzimuthMean")  else -1

                // TODO: why these default fallbacks?
                val szaTile = angleTile(szaIdx, if (overrideParams.get(0).isNaN) 29.0  else overrideParams.get(0))
                val vzaTile = angleTile(vzaIdx, if (overrideParams.get(1).isNaN) 5.0   else overrideParams.get(1))
                val saaTile = angleTile(saaIdx, if (overrideParams.get(2).isNaN) 130.0 else overrideParams.get(2))
                val vaaTile = angleTile(vaaIdx, 0.0) // because override provides raa == saa-vaa
                
                val raaTileDiff = saaTile - vaaTile
                val raaTile=raaTileDiff.mapDouble(v =>
                  ( if (v < -180.0) v+360.0 else if ( v > 180.0) v-360.0 else v ).abs  
                )

                val startMillis = System.currentTimeMillis();

                val demTile = if (overrideParams.get(3).isNaN) elevationProvider.compute(multibandtile._1, crs, layoutDefinition)
                              else FloatConstantTile(overrideParams.get(3).toFloat, multibandtile._2.cols, multibandtile._2.rows)
                val aotTile = if (overrideParams.get(4).isNaN) aotProvider.computeAOT(multibandtile._1, crs, layoutDefinition) 
                              else FloatConstantTile(overrideParams.get(4).toFloat, multibandtile._2.cols, multibandtile._2.rows)


                val afterAuxData = System.currentTimeMillis()
                auxDataAccum.add(afterAuxData-startMillis)

                val (cwvTile: Tile, result: MultibandTile) = correctTile(multibandtile, bandIds, szaTile, vzaTile, raaTile,aotTile, demTile, overrideParams, sensorDescriptor, cwvProvider)
                correctionAccum.add(System.currentTimeMillis() - afterAuxData)
                
                if (appendDebugBands)
                  MultibandTile(result.bands ++ Vector(
                      (szaTile*100).convert(multibandtile._2.cellType),
                      (vzaTile*100).convert(multibandtile._2.cellType),
                      (raaTile*100).convert(multibandtile._2.cellType),
                      (demTile*100).convert(multibandtile._2.cellType),
                      (aotTile*100).convert(multibandtile._2.cellType),
                      (cwvTile*100).convert(multibandtile._2.cellType)
                  ))
                else 
                  result
                
              }
            )
        }

      }),
      datacube.metadata
    )

  }

  private def correctTile(multibandtile: (SpaceTimeKey, MultibandTile), bandIds: util.List[String], szaTile: Tile, vzaTile: Tile, raaTile: Tile, aotTile: Tile, demTile: Tile, overrideParams: util.List[Double], sensorDescriptor: CorrectionDescriptor, cwvProvider: CWVProvider) = {
    // keep cwv last because depends on the others a lot
    val cwvTile = if (overrideParams.get(5).isNaN) cwvProvider.compute(multibandtile, szaTile, vzaTile, raaTile, demTile, 0.1, 0.33, 1.0e-4, 1.0, bandIds)
    else FloatConstantTile(overrideParams.get(5).toFloat, multibandtile._2.cols, multibandtile._2.rows)

    val result = multibandtile._2.mapBands((b, tile) => {
      val bandName = bandIds.get(b)
      try {
        val iband: Int = sensorDescriptor.getBandFromName(bandName)
        val resultTile: Tile = MultibandTile(
          tile.convert(FloatConstantNoDataCellType),
          aotTile.convert(FloatConstantNoDataCellType),
          demTile.convert(FloatConstantNoDataCellType),
          szaTile,
          vzaTile,
          raaTile,
          cwvTile
        ).combineDouble(0, 1, 2, 3, 4, 5, 6) { (refl, aot, dem, sza, vza, raa, cwv) => if (refl != NODATA) (sensorDescriptor.correct( iband, multibandtile._1.time, refl.toDouble, sza, vza, raa, dem, aot, cwv, overrideParams.get(6), 0)).toInt else NODATA }
        resultTile.convert(tile.cellType)
      } catch {
        case e: IllegalArgumentException => tile
      }

    })
    (cwvTile, result)
  }
}
