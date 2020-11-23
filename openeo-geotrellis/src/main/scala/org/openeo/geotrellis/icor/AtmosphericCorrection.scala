package org.openeo.geotrellis.icor

import java.util.concurrent.Callable

import com.google.common.cache.{Cache, CacheBuilder}
import geotrellis.layer._
import geotrellis.raster.{FloatConstantNoDataCellType, FloatConstantTile, MultibandTile, NODATA, Tile}
import geotrellis.spark._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.slf4j.LoggerFactory

object AtmosphericCorrection{
  implicit val logger = LoggerFactory.getLogger(classOf[AtmosphericCorrection])
  val lutCache: Cache[String, Broadcast[LookupTable]] = CacheBuilder.newBuilder().softValues().build()
}


class AtmosphericCorrection {

  import AtmosphericCorrection._

  def correct(
        jsc: JavaSparkContext, 
        datacube: MultibandTileLayerRDD[SpaceTimeKey], 
        tableId: String, 
        bandIds:java.util.List[String],
        prePostMult:java.util.List[Double],
        defParams:java.util.List[Double]
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

    val auxDataAccum = sc.longAccumulator("Icor aux data loading")
    val correctionAccum = sc.longAccumulator("Icor correction")

    new ContextRDD(
      datacube.mapPartitions(partition => {
        val aotProvider = new AOTProvider()
        val demProvider = new DEMProvider(layoutDefinition, crs)
        partition.map {
          multibandtile =>
            (
              multibandtile._1,
              //          multibandtile._2.mapBands((b, tile) => tile.map(i => 23 ))
              {

                val cd = new CorrectionDescriptorSentinel2()

                def angleTile(index: Int, fallback: Double): Tile = {
                  if (index > 0) multibandtile._2.band(index).convert(FloatConstantNoDataCellType) else FloatConstantTile(fallback.toFloat, multibandtile._2.cols, multibandtile._2.rows)
                }

                val saaIdx = bandIds.indexOf("sunAzimuthAngles")
                val szaIdx = bandIds.indexOf("sunZenithAngles")
                val vaaIdx = bandIds.indexOf("viewAzimuthMean")
                val vzaIdx = bandIds.indexOf("viewZenithMean")

                val szaTile = angleTile(szaIdx, defParams.get(0))
                val vzaTile = angleTile(vzaIdx, defParams.get(1))
                val vaaTile = angleTile(vaaIdx, 0.0)
                val saaTile = angleTile(saaIdx, 130.0)
                
                val raaTileDiff = saaTile - vaaTile
                val raaTile=raaTileDiff.mapDouble(v =>
                  ( if (v < -180.0) v+360.0 else if ( v > 180.0) v-360.0 else v ).abs  
                )

                val startMillis = System.currentTimeMillis();
                val aotTile = aotProvider.computeAOT(multibandtile._1, crs, layoutDefinition)
                val demTile = demProvider.computeDEM(multibandtile._1, crs, layoutDefinition)
                val afterAuxData = System.currentTimeMillis()
                auxDataAccum.add(afterAuxData-startMillis)

                val result = multibandtile._2.mapBands((b, tile) => {
                  val bandName = bandIds.get(b)
                  try {
                    val iband: Int = cd.getBandFromName(bandName)
                    val resultTile: Tile = MultibandTile(tile.convert(FloatConstantNoDataCellType), aotTile.convert(FloatConstantNoDataCellType), demTile.convert(FloatConstantNoDataCellType), szaTile, vzaTile, raaTile).combineDouble(0, 1, 2, 3, 4, 5) { (refl, aot, dem, sza, vza, raa) => if (refl != NODATA) (prePostMult.get(1) * cd.correct(bcLUT.value, iband, multibandtile._1.time, refl.toDouble * prePostMult.get(0), sza, vza, raa, dem, aot, defParams.get(5), defParams.get(6), 0)).toInt else NODATA }
                    resultTile.convert(tile.cellType)
                  } catch {
                    case e: IllegalArgumentException => tile
                  }

                })
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


