package org.openeo.geotrellis.geocoding

import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.raster.buffer.BufferedTile
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{CellSize, DoubleArrayTile, FloatConstantNoDataCellType, MultibandTile, Raster, RasterExtent}
import geotrellis.spark.tiling._
import geotrellis.spark._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD
import org.esa.snap.core.dataio.geocoding.GeoRaster
import org.esa.snap.core.dataio.geocoding.inverse.PixelQuadTreeInverse
import org.esa.snap.core.dataio.geocoding.util.XYInterpolator
import org.esa.snap.core.dataio.geocoding.util.XYInterpolator.SYSPROP_GEOCODING_INTERPOLATOR
import org.esa.snap.core.datamodel.{GeoPos, PixelPos}
import org.esa.snap.runtime.Config
import org.openeo.geotrelliscommon.DatacubeSupport
import org.slf4j.{Logger, LoggerFactory}

object GeoCodingProcess {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[GeoCodingProcess])
}

class GeoCodingProcess extends Serializable {



  def geoCode(inputTile: MultibandTile, crs:CRS, resolution:CellSize = CellSize(20.0,20.0), lonIndex:Int =3, latIndex:Int = 2): Option[Raster[MultibandTile]] = {

    val latitudes  = inputTile.band(latIndex).toArrayDouble()
    val longitudes = inputTile.band(lonIndex).toArrayDouble()


    Config.instance("snap").preferences.put(SYSPROP_GEOCODING_INTERPOLATOR, XYInterpolator.Type.GEODETIC.name)
    //val estimatedRes = estimateGroundResolutionInKm(latitudes,longitudes,croppedInput.cols,croppedInput.rows)
    val geoCoder = new PixelQuadTreeInverse.Plugin(true).create().asInstanceOf[PixelQuadTreeInverse]
    // 0.15 is estimated distance between pixels in km, the expected value for Sentinel1 would be around 0.02 (20m)
    // if we set it to such a lower value however, gaps appear in the output
    val geoRaster = new GeoRaster(longitudes, latitudes, "lon", "lat", inputTile.cols, inputTile.rows, 0.02)
    geoCoder.initialize(geoRaster, false, Array.empty[PixelPos])

    val pixelPos = new PixelPos()

    val minLon = longitudes.min
    val maxLon = longitudes.max
    val minLat = latitudes.min
    val maxLat = latitudes.max

    if (minLat.isNaN || maxLat.isNaN || minLon.isNaN || maxLon.isNaN) {
      return None
    }

    val reprojected = Extent(minLon, minLat, maxLon, maxLat).reproject(LatLng, crs) //.buffer(-10000.0) //.buffer(-20000.0,0.0)

    val re = RasterExtent(reprojected, resolution)
    val tile = DoubleArrayTile.ofDim(re.cols, re.rows)
    val coordTransform = Transform(crs, LatLng)

    val geoCoded = tile.mapDouble { (x, y, d) => {

      val (xCoord, yCoord) = re.gridToMap(x, y)
      val (lon, lat) = coordTransform(xCoord, yCoord)

      val resultPos = geoCoder.getPixelPos(new GeoPos(lat, lon), pixelPos)
      if (resultPos.isValid) {
        try {

          inputTile.band(0).getDouble(resultPos.x.round.toInt, resultPos.y.round.toInt)
        } catch {
          case e: ArrayIndexOutOfBoundsException =>
            GeoCodingProcess.logger.error(s"resample_spatial - geocode: Error retrieving value for pixel position  ($lon, $lat) - ($xCoord, $yCoord ) : ${e.getMessage}")
            Double.NaN
        }
      } else {
        Double.NaN
      }
    }
    }
    Some(Raster(MultibandTile(geoCoded), re.extent))

  }

  def geoCode(cube: MultibandTileLayerRDD[SpaceTimeKey], targetExtent: Extent, targetCRS: CRS, targetResolution: CellSize): MultibandTileLayerRDD[SpaceTimeKey] = {

    val bandLabels = DatacubeSupport.maybeBandLabels(cube).getOrElse{throw new IllegalArgumentException("Band labels missing from input cube, cannot proceed with geocoding.")}

    if( !bandLabels.contains("latitude") || !bandLabels.contains("longitude")){
      throw new IllegalArgumentException(s"resample_spatial - geocode: Input cube does not contain latitude and longitude bands, cannot proceed with geocoding. Band labels: ${bandLabels.mkString(",")}")
    }
    val latIndex = bandLabels.indexOf("latitude")
    val lonIndex = bandLabels.indexOf("longitude")

    val minmaxValues: RDD[Vector[(Double, Double)]] = cube.mapValues(_.subsetBands(lonIndex,latIndex)).map(_._2.bands.map(_.findMinMaxDouble))
    val minLon = minmaxValues.map(_(0)._1).min()
    val maxLon = minmaxValues.map(_(0)._2).max()
    val minLat = minmaxValues.map(_(1)._1).min()
    val maxLat = minmaxValues.map(_(1)._2).max()

    val localTargetExtent = ProjectedExtent(Extent(minLon, minLat, maxLon, maxLat),LatLng).reproject(targetCRS)
    val bufferedCube: RDD[(SpaceTimeKey, BufferedTile[MultibandTile])] = cube.bufferTiles(32)
    val rasters: RDD[(TemporalProjectedExtent, MultibandTile)] = bufferedCube.flatMap { case (key: SpaceTimeKey, tile: BufferedTile[MultibandTile]) => {

      val raster = geoCode(tile.tile, targetCRS,targetResolution, lonIndex,latIndex)
      //if(raster.isDefined) {
      //  GeoTiff(raster.get, targetCRS).write(s"/tmp/geocoded_${key.time}_${key.spatialKey.col}_${key.spatialKey.row}.tif")
      //}
      raster.map(r=>(TemporalProjectedExtent(r.extent, targetCRS, key.time), r.tile))
    }
    }
    val targetLayout: LayoutDefinition = LayoutDefinition(RasterExtent(localTargetExtent, targetResolution), 256, 256)
    val origBounds = cube.metadata.bounds.get

    val md = DatacubeSupport.tileLayerMetadata(targetLayout, ProjectedExtent(localTargetExtent, targetCRS), origBounds.minKey.time, origBounds.maxKey.time, FloatConstantNoDataCellType)

    val tiled: RDD[(SpaceTimeKey, MultibandTile)] = rasters.tileToLayout(FloatConstantNoDataCellType, targetLayout, Tiler.Options(NearestNeighbor))
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(tiled.groupByKey().mapValues(_.reduce(_ merge (_))), md)
    tiledRDD
  }

}

