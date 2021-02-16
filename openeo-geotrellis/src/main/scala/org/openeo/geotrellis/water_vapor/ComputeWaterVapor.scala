package org.openeo.geotrellis.water_vapor

import java.util.concurrent.Callable

import com.google.common.cache.{Cache, CacheBuilder}
import geotrellis.layer._
import geotrellis.raster.{FloatConstantNoDataCellType, FloatConstantTile, MultibandTile, Tile}
import geotrellis.spark._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.openeo.geotrellis.icor._
import org.slf4j.LoggerFactory



object ComputeWaterVapor{
  implicit val logger = LoggerFactory.getLogger(classOf[ComputeWaterVapor])
  val lutCache: Cache[String, Broadcast[LookupTable]] = CacheBuilder.newBuilder().softValues().build()
}


class ComputeWaterVapor {

  import ComputeWaterVapor._
    
  def computeStandaloneCWV(
        jsc: JavaSparkContext, 
        datacube: MultibandTileLayerRDD[SpaceTimeKey], 
        bandIds:java.util.List[String], // 
        prePostMult:java.util.List[Double], // [1.e-4,1.]
        defParams:java.util.List[Double], // [ sza, saa, vza, vaa, aot (fixed override)=0.1, ozone (fixed override)=0.33 ]
        sensorId: String // SENTINEL2 and LANDSAT8 for now but in the future SENTINEL2A,SENTINEL2B,... granulation will be needed
      ): ContextRDD[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]]  = {

    val sc = JavaSparkContext.toSparkContext(jsc)

    val sensorDescriptor: ICorCorrectionDescriptor = sensorId.toUpperCase() match {
      case "SENTINEL2"  => new Sentinel2Descriptor()
      case "LANDSAT8"   => new Landsat8Descriptor()
    }
    
    val lutLoader = new Callable[Broadcast[LookupTable]]() {
      override def call(): Broadcast[LookupTable] = {
        org.openeo.geotrellis.logTiming("Loading icor LUT")({
          sc.broadcast(LookupTableIO.readLUT(sensorDescriptor.getLookupTableURL()))
        })
      }
    }
    val bcLUT = lutCache.get(sensorDescriptor.getLookupTableURL(), lutLoader)

    val crs = datacube.metadata.crs
    val layoutDefinition = datacube.metadata.layout

    val auxDataAccum = sc.longAccumulator("Icor water vapor data loading")
    val correctionAccum = sc.longAccumulator("Icor water vapor calculator")

    new ContextRDD(
      datacube.mapPartitions(partition => {
// AOT overriden        val aotProvider = new AOTProvider()
        val demProvider = new DEMProvider(layoutDefinition, crs)
        val cwvProvider = new CWVProvider(sensorDescriptor)
        
        partition.map {
          multibandtile =>
            (
              multibandtile._1,
              //          multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
              {

                val startMillis = System.currentTimeMillis();

                def angleTile(index: Int, fallback: Double): Tile = {
                  if (index > 0) multibandtile._2.band(index).convert(FloatConstantNoDataCellType) else FloatConstantTile(fallback.toFloat, multibandtile._2.cols, multibandtile._2.rows)
                }
                
                val szaIdx = bandIds.indexOf("sunZenithAngles")
                val saaIdx = bandIds.indexOf("sunAzimuthAngles")
                val vzaIdx = bandIds.indexOf("viewZenithMean")
                val vaaIdx = bandIds.indexOf("viewAzimuthMean")
                
                val szaTile = angleTile(szaIdx, defParams.get(0))
                val saaTile = angleTile(saaIdx, defParams.get(1))
                val vzaTile = angleTile(vzaIdx, defParams.get(2))
                val vaaTile = angleTile(vaaIdx, defParams.get(3))
                
                val raaTileDiff = saaTile - vaaTile
                val raaTile=raaTileDiff.mapDouble(v =>
                  ( if (v < -180.0) v+360.0 else if ( v > 180.0) v-360.0 else v ).abs  
                )

                val demTile = demProvider.compute(multibandtile._1, crs, layoutDefinition)

                // AOT overriden                val aotTile = aotProvider.computeAOT(multibandtile._1, crs, layoutDefinition)
                val aot=defParams.get(4)
                val ozone=defParams.get(5)

                val afterAuxData = System.currentTimeMillis()
                auxDataAccum.add(afterAuxData-startMillis)
                
                val result=MultibandTile(cwvProvider.compute(
                  multibandtile,
                  szaTile,
                  vzaTile,
                  raaTile,
                  demTile,
                  aot,
                  ozone,
                  prePostMult.get(0),
                  prePostMult.get(1),
                  bandIds
                ))
                
                correctionAccum.add(System.currentTimeMillis() - afterAuxData)
                
                result
              }
            )
        }

      }),
      datacube.metadata
    )

  }  
  
}


