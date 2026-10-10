package org.openeo.geotrellis.geocoding


import geotrellis.layer.{KeyBounds, LayoutDefinition, Metadata, SpaceTimeKey, SpatialKey, TemporalProjectedExtent, TileLayerMetadata}
import geotrellis.proj4.{CRS, LatLng, Transform, WebMercator}
import geotrellis.raster.{CellSize, DoubleArrayTile, FloatConstantNoDataCellType, GridBounds, MultibandTile, Raster, RasterExtent, Tile}
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.NearestNeighbor
import geotrellis.spark.{ContextRDD, MultibandTileLayerRDD, withTilerMethods}
import geotrellis.spark.util.SparkUtils
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.openeo.geotrellis.OpenEOProcesses
import org.openeo.geotrellis.geotiff.{saveRDD, saveRDDTemporal}
import org.openeo.geotrelliscommon.DatacubeSupport
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Path
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
  def testCoordinateBoundsIgnoresNoData(): Unit = {
    val nan = Double.NaN
    val lons = Array(nan, -999.0, 4.0, 5.0, nan, 6.0)
    val lats = Array(nan, 50.0, -999.0, 51.0, nan, 52.0)
    assertEquals(Some(Extent(4.0, 50.0, 6.0, 52.0)), GeoCodingProcess.coordinateBounds(lons, lats))

    assertEquals(None, GeoCodingProcess.coordinateBounds(Array(nan, -999.0), lats))
    assertEquals(None, GeoCodingProcess.coordinateBounds(lons, Array(nan, nan)))
  }

  @Test
  def testValidCoordinateWindow(): Unit = {
    val nan = Double.NaN
    // 3x3 grid, only (col 1..2, row 1..2) has both a valid lon and lat
    val lons = Array(nan, nan, nan, nan, 4.0, 4.1, nan, 4.0, 4.1)
    val lats = Array(nan, 51.0, nan, -999.0, 51.0, 51.0, nan, 50.9, 50.9)
    assertEquals(Some(GridBounds(1, 1, 2, 2)), GeoCodingProcess.validCoordinateWindow(lons, lats, 3))
    assertEquals(None, GeoCodingProcess.validCoordinateWindow(Array(nan, 4.0), Array(51.0, nan), 2))
  }

  @Test
  def testGeoCodeTileWithNoDataBorder(): Unit = {
    val size = 16
    val border = 4
    def coordTile(f: (Int, Int) => Double) = DoubleArrayTile(Array.tabulate(size * size) { i =>
      val (col, row) = (i % size, i / size)
      if (col < border || row < border) Double.NaN else f(col, row)
    }, size, size)

    val vv = DoubleArrayTile(Array.fill(size * size)(1.0), size, size)
    val vh = DoubleArrayTile(Array.fill(size * size)(2.0), size, size)
    val lats = coordTile((_, row) => 51.0 - row * 0.001)
    val lons = coordTile((col, _) => 4.0 + col * 0.001)
    val input = MultibandTile(vv, vh, lats, lons)

    val result = new GeoCodingProcess().geoCode(input, CRS.fromEpsgCode(32631), CellSize(20.0, 20.0))
    assertTrue(result.isDefined, "tile with a NoData border should still be geocoded")

    val geoCoded = result.get.tile
    assertEquals(2, geoCoded.bandCount, "all non-coordinate bands should be geocoded")
    def validValues(band: Int) = geoCoded.band(band).toArrayDouble().filterNot(_.isNaN).distinct.toSeq
    assertEquals(Seq(1.0), validValues(0))
    assertEquals(Seq(2.0), validValues(1))
  }

  @Test
  def testGeoCodeCube(@TempDir tempDir: Path): Unit = {

    val resource = Thread.currentThread().getContextClassLoader.getResource("org/openeo/geotrellis/geocoding/coherence_master.tif")

    val masterTiff = GeoTiff.readMultiband(resource.toString.stripPrefix("file:"))

    val inputLayout:LayoutDefinition = LayoutDefinition(masterTiff.rasterExtent, 128, 128)


    val tiledInput: RDD[(SpaceTimeKey, MultibandTile)] = GeoCodingTest.sc.parallelize(Seq((TemporalProjectedExtent(masterTiff.extent,masterTiff.crs, 0L),masterTiff.tile))).tileToLayout(FloatConstantNoDataCellType,inputLayout)

    val inputMetadata = DatacubeSupport.tileLayerMetadata(inputLayout,masterTiff.projectedExtent,ZonedDateTime.now(),ZonedDateTime.now(),FloatConstantNoDataCellType)

    val cube: MultibandTileLayerRDD[SpaceTimeKey] = ContextRDD(tiledInput,inputMetadata)
    val targetExtent = Extent(1078161.262, 5197478.538, 1176612.520, 5228026.100)
    val targetCRS = CRS.fromEpsgCode(32631)
    val wrapped = new OpenEOProcesses().wrapCube(cube)
    wrapped.openEOMetadata.setBandNames(util.Arrays.asList("VV","VH","latitude","longitude"))
    val tiledRDD: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]] = new GeoCodingProcess().geoCode(wrapped, targetExtent, targetCRS, CellSize(20.0,20.0))

    assertEquals(Some(Seq("VV", "VH")), DatacubeSupport.maybeBandLabels(tiledRDD))
    assertTrue(tiledRDD.values.collect().forall(_.bandCount == 2), "every output tile should contain VV and VH")

    saveRDDTemporal(tiledRDD, tempDir.resolve("geocoded_cube.tif").toAbsolutePath.toString)
  }
}
