package org.openeo.geotrellis.geocoding

import geotrellis.layer.{LayoutDefinition, Metadata, SpaceTimeKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng, Transform}
import geotrellis.raster.buffer.BufferedTile
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.raster.{CellSize, DoubleArrayTile, FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, RasterExtent}
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD
import org.esa.snap.core.dataio.geocoding.GeoRaster
import org.esa.snap.core.dataio.geocoding.inverse.PixelQuadTreeInverse
import org.esa.snap.core.dataio.geocoding.util.XYInterpolator
import org.esa.snap.core.dataio.geocoding.util.XYInterpolator.SYSPROP_GEOCODING_INTERPOLATOR
import org.esa.snap.core.datamodel.{GeoPos, PixelPos}
import org.esa.snap.runtime.Config
import org.openeo.geotrelliscommon.{DatacubeSupport, OpenEORasterCube, OpenEORasterCubeMetadata}
import org.slf4j.{Logger, LoggerFactory}

object GeoCodingProcess {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[GeoCodingProcess])

  private val CoordinateNoData = -999.0

  private def isValidCoordinate(v: Double): Boolean = !v.isNaN && v != CoordinateNoData

  private def validRange(values: Array[Double]): Option[(Double, Double)] = {
    var min = Double.PositiveInfinity
    var max = Double.NegativeInfinity
    var i = 0
    while (i < values.length) {
      val v = values(i)
      if (isValidCoordinate(v)) {
        if (v < min) min = v
        if (v > max) max = v
      }
      i += 1
    }
    if (min <= max) Some((min, max)) else None
  }

  /**
   * Smallest pixel window containing every pixel with a valid lon and lat. SNAP's inverse geocoding
   * finds no positions at all when its coordinate grid has invalid borders (e.g. from bufferTiles).
   */
  private[geocoding] def validCoordinateWindow(longitudes: Array[Double], latitudes: Array[Double], cols: Int): Option[GridBounds[Int]] = {
    var colMin = Int.MaxValue
    var colMax = Int.MinValue
    var rowMin = Int.MaxValue
    var rowMax = Int.MinValue
    var i = 0
    while (i < longitudes.length) {
      if (isValidCoordinate(longitudes(i)) && isValidCoordinate(latitudes(i))) {
        val col = i % cols
        val row = i / cols
        if (col < colMin) colMin = col
        if (col > colMax) colMax = col
        if (row < rowMin) rowMin = row
        if (row > rowMax) rowMax = row
      }
      i += 1
    }
    if (colMin <= colMax) Some(GridBounds(colMin, rowMin, colMax, rowMax)) else None
  }

  /** Lon/lat bounds of the valid coordinates, ignoring NaN/NoData and the -999 sentinel. */
  private[geocoding] def coordinateBounds(longitudes: Array[Double], latitudes: Array[Double]): Option[Extent] =
    for {
      (minLon, maxLon) <- validRange(longitudes)
      (minLat, maxLat) <- validRange(latitudes)
    } yield Extent(minLon, minLat, maxLon, maxLat)

  private def combine(a: Option[Extent], b: Option[Extent]): Option[Extent] = (a ++ b).reduceOption(_ combine _)

  /** Indices of the bands to geocode: every band except the longitude and latitude bands. */
  private[geocoding] def valueBandIndices(bandCount: Int, lonIndex: Int, latIndex: Int): Seq[Int] =
    (0 until bandCount).filter(i => i != lonIndex && i != latIndex)
}

class GeoCodingProcess extends Serializable {

  def geoCode(input: MultibandTile, crs: CRS, resolution: CellSize = CellSize(20.0, 20.0), lonIndex: Int = 3, latIndex: Int = 2): Option[Raster[MultibandTile]] = {

    val window = GeoCodingProcess.validCoordinateWindow(input.band(lonIndex).toArrayDouble(), input.band(latIndex).toArrayDouble(), input.cols) match {
      case Some(w) => w
      case None => return None
    }
    val inputTile = if (window == GridBounds(0, 0, input.cols - 1, input.rows - 1)) input else input.crop(window)

    val latitudes = inputTile.band(latIndex).toArrayDouble()
    val longitudes = inputTile.band(lonIndex).toArrayDouble()


    Config.instance("snap").preferences.put(SYSPROP_GEOCODING_INTERPOLATOR, XYInterpolator.Type.GEODETIC.name)
    //val estimatedRes = estimateGroundResolutionInKm(latitudes,longitudes,croppedInput.cols,croppedInput.rows)
    val geoCoder = new PixelQuadTreeInverse.Plugin(true).create().asInstanceOf[PixelQuadTreeInverse]
    // 0.15 is estimated distance between pixels in km, the expected value for Sentinel1 would be around 0.02 (20m)
    // if we set it to such a lower value however, gaps appear in the output
    val geoRaster = new GeoRaster(longitudes, latitudes, "lon", "lat", inputTile.cols, inputTile.rows, 0.02)
    geoCoder.initialize(geoRaster, false, Array.empty[PixelPos])

    val pixelPos = new PixelPos()

    val lonLatBounds = GeoCodingProcess.coordinateBounds(longitudes, latitudes) match {
      case Some(bounds) => bounds
      case None => return None
    }

    val reprojected = lonLatBounds.reproject(LatLng, crs) //.buffer(-10000.0) //.buffer(-20000.0,0.0)

    val re = RasterExtent(reprojected, resolution)
    val coordTransform = Transform(crs, LatLng)

    // Resolve the inverse geocoding once per output pixel; -1 marks pixels without a valid source position.
    val sourceIndices = new Array[Int](re.cols * re.rows)
    var row = 0
    while (row < re.rows) {
      var col = 0
      while (col < re.cols) {
        val (xCoord, yCoord) = re.gridToMap(col, row)
        val (lon, lat) = coordTransform(xCoord, yCoord)
        val resultPos = geoCoder.getPixelPos(new GeoPos(lat, lon), pixelPos)
        sourceIndices(row * re.cols + col) =
          if (resultPos.isValid) {
            val srcCol = resultPos.x.round.toInt
            val srcRow = resultPos.y.round.toInt
            if (srcCol >= 0 && srcCol < inputTile.cols && srcRow >= 0 && srcRow < inputTile.rows) srcRow * inputTile.cols + srcCol
            else {
              GeoCodingProcess.logger.debug(s"resample_spatial - geocode: pixel position ($srcCol, $srcRow) for ($lon, $lat) is outside the input tile")
              -1
            }
          } else -1
        col += 1
      }
      row += 1
    }

    val valueBandIndices = GeoCodingProcess.valueBandIndices(inputTile.bandCount, lonIndex, latIndex)
    val geoCodedBands = valueBandIndices.map { bandIndex =>
      val band = inputTile.band(bandIndex)
      val values = sourceIndices.map(i => if (i < 0) Double.NaN else band.getDouble(i % inputTile.cols, i / inputTile.cols))
      DoubleArrayTile(values, re.cols, re.rows)
    }
    Some(Raster(MultibandTile(geoCodedBands), re.extent))

  }

  def geoCode(cube: MultibandTileLayerRDD[SpaceTimeKey], targetExtent: Extent, targetCRS: CRS, targetResolution: CellSize): MultibandTileLayerRDD[SpaceTimeKey] = {

    val bandLabels = DatacubeSupport.maybeBandLabels(cube).getOrElse {
      throw new IllegalArgumentException("Band labels missing from input cube, cannot proceed with geocoding.")
    }

    if (!bandLabels.contains("latitude") || !bandLabels.contains("longitude")) {
      throw new IllegalArgumentException(s"resample_spatial - geocode: Input cube does not contain latitude and longitude bands, cannot proceed with geocoding. Band labels: ${bandLabels.mkString(",")}")
    }
    val latIndex = bandLabels.indexOf("latitude")
    val lonIndex = bandLabels.indexOf("longitude")
    val outputBandLabels = GeoCodingProcess.valueBandIndices(bandLabels.size, lonIndex, latIndex).map(bandLabels)
    if (outputBandLabels.isEmpty) {
      throw new IllegalArgumentException(s"resample_spatial - geocode: Input cube contains only latitude and longitude bands, nothing to geocode. Band labels: ${bandLabels.mkString(",")}")
    }

    val lonLatBounds: Extent = cube
      .map { case (_, tile) => GeoCodingProcess.coordinateBounds(tile.band(lonIndex).toArrayDouble(), tile.band(latIndex).toArrayDouble()) }
      .fold(None)(GeoCodingProcess.combine)
      .getOrElse(throw new IllegalArgumentException("resample_spatial - geocode: Input cube does not contain any valid latitude/longitude values, cannot proceed with geocoding."))

    val localTargetExtent = ProjectedExtent(lonLatBounds, LatLng).reproject(targetCRS)
    val bufferedCube: RDD[(SpaceTimeKey, BufferedTile[MultibandTile])] = cube.bufferTiles(32)
    val rasters: RDD[(TemporalProjectedExtent, MultibandTile)] = bufferedCube.flatMap { case (key: SpaceTimeKey, tile: BufferedTile[MultibandTile]) => {

      val raster = geoCode(tile.tile, targetCRS, targetResolution, lonIndex, latIndex)
      //if(raster.isDefined) {
      //  GeoTiff(raster.get, targetCRS).write(s"/tmp/geocoded_${key.time}_${key.spatialKey.col}_${key.spatialKey.row}.tif")
      //}
      raster.map(r => (TemporalProjectedExtent(r.extent, targetCRS, key.time), r.tile))
    }
    }
    val targetLayout: LayoutDefinition = LayoutDefinition(RasterExtent(localTargetExtent, targetResolution), 256, 256)
    val origBounds = cube.metadata.bounds.get

    val md = DatacubeSupport.tileLayerMetadata(targetLayout, ProjectedExtent(localTargetExtent, targetCRS), origBounds.minKey.time, origBounds.maxKey.time, FloatConstantNoDataCellType)

    val tiled: RDD[(SpaceTimeKey, MultibandTile)] = rasters.tileToLayout(FloatConstantNoDataCellType, targetLayout, Tiler.Options(NearestNeighbor))
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] =
      new OpenEORasterCube(tiled.reduceByKey(_ merge _), md, new OpenEORasterCubeMetadata(outputBandLabels))
    tiledRDD
  }

}

