package org.openeo.geotrellis.geocoding


import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng, Transform, WebMercator}
import geotrellis.raster.{CellSize, DoubleArrayTile, FloatConstantNoDataCellType, MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, withTilerMethods}

import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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
    val raster = new GeoCodingProcess().geocode(inputTile,utm).get

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
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = new GeoCodingProcess().geoCode(cube, targetExtent, targetCRS)

    saveRDDTemporal(tiledRDD, "/tmp/geocoded_cube.tif")




  }


}
