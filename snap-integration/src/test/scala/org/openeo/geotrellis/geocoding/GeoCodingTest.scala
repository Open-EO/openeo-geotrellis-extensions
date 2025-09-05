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
import org.openeo.geotrellis.OpenEOProcesses
import org.openeo.geotrellis.geotiff.{saveRDD, saveRDDTemporal}
import org.openeo.geotrelliscommon.DatacubeSupport
import org.slf4j.{Logger, LoggerFactory}

import java.time.{ZoneOffset, ZonedDateTime}
import java.util

object GeoCodingTest{

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[GeoCodingTest])
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

class GeoCodingTest {



  @Test
  def testGeoCodeCube(): Unit = {

    val resource = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/geocoding/coherence_master.tif")

    val masterTiff = GeoTiff.readMultiband(resource.toString.stripPrefix("file:"))

    val inputLayout:LayoutDefinition = LayoutDefinition(masterTiff.rasterExtent, 256, 256)


    val tiledInput: RDD[(SpaceTimeKey, MultibandTile)] = GeoCodingTest.sc.parallelize(Seq((TemporalProjectedExtent(masterTiff.extent,masterTiff.crs, 0L),masterTiff.tile))).tileToLayout(FloatConstantNoDataCellType,inputLayout)

    val inputMetadata = DatacubeSupport.tileLayerMetadata(inputLayout,masterTiff.projectedExtent,ZonedDateTime.now(),ZonedDateTime.now(),FloatConstantNoDataCellType)

    val cube: MultibandTileLayerRDD[SpaceTimeKey] = ContextRDD(tiledInput,inputMetadata)
    val targetExtent = Extent(1078161.262, 5197478.538, 1176612.520, 5228026.100)
    val targetCRS = CRS.fromEpsgCode(32631)
    val wrapped = new OpenEOProcesses().wrapCube(cube)
    wrapped.openEOMetadata.setBandNames(util.Arrays.asList("VV","VH","latitude","longitude"))
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = new GeoCodingProcess().geoCode(wrapped, targetExtent, targetCRS, CellSize(20.0,20.0))

    saveRDDTemporal(tiledRDD, "/tmp/geocoded_cube.tif")


  }


}
