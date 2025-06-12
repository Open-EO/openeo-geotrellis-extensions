package org.openeo.geotrellis.geocoding


import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng, Transform, WebMercator}
import geotrellis.raster.{CellSize, DoubleArrayTile, FloatConstantNoDataCellType, MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, withTilerMethods}
import geotrellis.spark.tiling._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.esa.snap.core.dataio.geocoding.GeoRaster
import org.esa.snap.core.dataio.geocoding.inverse.PixelQuadTreeInverse
import org.esa.snap.core.datamodel.{GeoPos, PixelPos}
import org.junit.jupiter.api.Test
import org.openeo.geotrellis.geotiff.{saveRDD, saveRDDTemporal}
import org.openeo.geotrelliscommon.DatacubeSupport
import org.slf4j.{Logger, LoggerFactory}

import java.time.{ZoneOffset, ZonedDateTime}

object TestGeoCoding{

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[TestGeoCoding])
  protected var _sc: Option[SparkContext] = None

  implicit def sc: SparkContext = {
    if (_sc.isEmpty) {
      val conf = new SparkConf()
        .set("spark.kryoserializer.buffer.max", "512m")
        .set("spark.rdd.compress", "true")
        .set("spark.ui.enabled", "true")
      _sc = Some(SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, conf))
      if (sc.uiWebUrl.isDefined) logger.info("Spark uiWebUrl: " + sc.uiWebUrl.get)
    }
    _sc.get
  }

  private def geocode(inputTile: MultibandTile, crs:CRS): Option[Raster[MultibandTile]] = {
    val geoCoder: PixelQuadTreeInverse = new PixelQuadTreeInverse.Plugin(false).create().asInstanceOf[PixelQuadTreeInverse]

    val latitudes = inputTile.band(2).toArrayDouble()
    val longitudes = inputTile.band(3).toArrayDouble()
    val geoRaster = new GeoRaster(longitudes, latitudes, "lon", "lat", inputTile.cols, inputTile.rows, 0.1)
    geoCoder.initialize(geoRaster, false, Array.empty[PixelPos])
    val pixelPos = new PixelPos()

    val minLon = longitudes.min
    val maxLon = longitudes.max
    val minLat = latitudes.min
    val maxLat = latitudes.max

    if(minLat.isNaN || maxLat.isNaN || minLon.isNaN || maxLon.isNaN) {

      return None
    }

    val reprojected = Extent(minLon, minLat, maxLon, maxLat).reproject(LatLng, crs)//.buffer(-10000.0) //.buffer(-20000.0,0.0)

    val re = RasterExtent(reprojected, CellSize(20.0, 20.0))
    val tile = DoubleArrayTile.ofDim(re.cols, re.rows)
    val coordTransform = Transform(crs, LatLng)

    val geoCoded = tile.mapDouble { (x, y, d) => {

      val (xCoord, yCoord) = re.gridToMap(x, y)
      val (lon, lat) = coordTransform(xCoord, yCoord)

      val resultPos = geoCoder.getPixelPos(new GeoPos(lat, lon), pixelPos)
      //println(resultPos)
      if (resultPos.isValid) {
        try{

          inputTile.band(0).getDouble(resultPos.x.round.toInt, resultPos.y.round.toInt)
        } catch {
          case e: ArrayIndexOutOfBoundsException =>
            logger.error(s"Error retrieving value for pixel position $resultPos: ${e.getMessage}")
            Double.NaN
        }
      } else {
        Double.NaN
      }
    }
    }
    Some(Raster(MultibandTile(geoCoded),re.extent))

  }

  private def geoCode(cube: MultibandTileLayerRDD[SpaceTimeKey], targetExtent: Extent, targetCRS: CRS) = {
    val rasters: RDD[(TemporalProjectedExtent, MultibandTile)] = cube.flatMap { case (key: SpaceTimeKey, tile: MultibandTile) => {

      val raster = geocode(tile, targetCRS)
      raster.map(r=>(TemporalProjectedExtent(r.extent, targetCRS, key.time), r.tile))
    }
    }
    val targetLayout: LayoutDefinition = LayoutDefinition(RasterExtent(targetExtent, CellSize(20.0, 20.0)), 256, 256)
    val origBounds = cube.metadata.bounds.get

    val md = DatacubeSupport.tileLayerMetadata(targetLayout, ProjectedExtent(targetExtent, targetCRS), origBounds.minKey.time, origBounds.maxKey.time, FloatConstantNoDataCellType)

    val tiled: RDD[(SpaceTimeKey, MultibandTile)] = rasters.tileToLayout(FloatConstantNoDataCellType, targetLayout, Tiler.Options(NearestNeighbor))
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = ContextRDD(tiled.groupByKey().mapValues(_.reduce(_ merge (_))), md)
    tiledRDD
  }
}

class TestGeoCoding {

  @Test
  def testBasic(): Unit = {
    // This is a placeholder for the actual test implementation.
    // You would typically use a testing framework like ScalaTest or JUnit to write your tests.
    // For example, you could check if a geocoding service returns expected results for given inputs.
    assert(true) // Replace with actual assertions


    val resource = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/geocoding/coherence_master.tif")

    val masterTiff = GeoTiff.readMultiband(resource.toString.stripPrefix("file:")).raster
    val inputTile = masterTiff.tile
    val utm = CRS.fromEpsgCode(32631)
    val raster = TestGeoCoding.geocode(inputTile,utm).get

    GeoTiff(raster, utm).write("/tmp/geocoded.tif")

  }



  @Test
  def testGeoCodeCube(): Unit = {

    val resource = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/geocoding/coherence_master.tif")

    val masterTiff = GeoTiff.readMultiband(resource.toString.stripPrefix("file:"))

    val inputLayout:LayoutDefinition = LayoutDefinition(masterTiff.rasterExtent, 128, 128)


    val tiledInput: RDD[(SpaceTimeKey, MultibandTile)] = TestGeoCoding.sc.parallelize(Seq((TemporalProjectedExtent(masterTiff.extent,masterTiff.crs, 0L),masterTiff.tile))).tileToLayout(FloatConstantNoDataCellType,inputLayout)

    val inputMetadata = DatacubeSupport.tileLayerMetadata(inputLayout,masterTiff.projectedExtent,ZonedDateTime.now(),ZonedDateTime.now(),FloatConstantNoDataCellType)

    val cube: MultibandTileLayerRDD[SpaceTimeKey] = ContextRDD(tiledInput,inputMetadata)
    val targetExtent = Extent(1078161.262, 5197478.538, 1176612.520, 5228026.100)
    val targetCRS = CRS.fromEpsgCode(32631)
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = TestGeoCoding.geoCode(cube, targetExtent, targetCRS)

    saveRDDTemporal(tiledRDD, "/tmp/geocoded_cube.tif")




  }


}
