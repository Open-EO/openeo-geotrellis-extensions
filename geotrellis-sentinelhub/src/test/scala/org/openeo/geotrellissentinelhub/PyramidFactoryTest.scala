package org.openeo.geotrellissentinelhub

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.LatLng
import geotrellis.proj4.util.UTM
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.{CellSize, HasNoData, MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.junit.Assert._
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue, fail}
import org.junit.Test
import org.openeo.geotrellissentinelhub.SampleType.{FLOAT32, SampleType}

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._

object PyramidFactoryTest {
  implicit class WithRootCause(e: Throwable) {
    def getRootCause: Throwable = if (e.getCause == null) e else e.getCause.getRootCause
  }

  private class CatalogApiSpy(endpoint: String) extends CatalogApi {
    private val catalogApi = new DefaultCatalogApi(endpoint)
    private val dateTimesCounter = new AtomicLong

    override def dateTimes(collectionId: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime,
                           accessToken: String, queryProperties: collection.Map[String, String]):
    Seq[ZonedDateTime] = {
      dateTimesCounter.incrementAndGet()
      catalogApi.dateTimes(collectionId, boundingBox, from, to, accessToken, queryProperties)
    }

    override def searchCard4L(collectionId: String, boundingBox: ProjectedExtent, from: ZonedDateTime,
                              to: ZonedDateTime, accessToken: String,
                              queryProperties: collection.Map[String, String]):
    Map[String, Feature[Geometry, ZonedDateTime]] = {
      catalogApi.searchCard4L(collectionId, boundingBox, from, to, accessToken, queryProperties)
    }

    def dateTimesCount: Long = dateTimesCounter.get()
  }

  private class ProcessApiSpy(endpoint: String)(implicit sc: SparkContext) extends ProcessApi with Serializable {
    private val processApi = new DefaultProcessApi(endpoint)
    private val getTileCounter = sc.longAccumulator

    override def getTile(datasetId: String, projectedExtent: ProjectedExtent, date: ZonedDateTime, width: Int,
                         height: Int, bandNames: Seq[String], sampleType: SampleType,
                         additionalDataFilters: util.Map[String, Any], processingOptions: util.Map[String, Any],
                         accessToken: String): MultibandTile = {
      getTileCounter.add(1)
      processApi.getTile(datasetId, projectedExtent, date, width, height, bandNames, sampleType,
        additionalDataFilters, processingOptions, accessToken)
    }

    def getTileCount: Long = getTileCounter.value
  }
}

class PyramidFactoryTest {
  import PyramidFactoryTest._

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret

  @Test
  def testGamma0(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 10, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    def testCellType(baseLayer: MultibandTileLayerRDD[SpaceTimeKey]): Unit = baseLayer.metadata.cellType match {
      case cellType: HasNoData[Double @unchecked] if cellType.isFloatingPoint && cellType.noDataValue == 0.0 =>
      case wrongCellType => fail(s"wrong CellType $wrongCellType")
    }

    testLayer(new PyramidFactory("sentinel-1-grd", "S1GRD", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, sampleType = FLOAT32, maxSpatialResolution = CellSize(10,10)), "gamma0_catalog", date,
      Seq("VV", "VH", "dataMask"), testCellType)
  }

  @Test
  def testSentinel2L1C(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    def testCellType(baseLayer: MultibandTileLayerRDD[SpaceTimeKey]): Unit = baseLayer.metadata.cellType match {
      case cellType: HasNoData[Int @unchecked] if !cellType.isFloatingPoint && cellType.noDataValue == 0 =>
      case wrongCellType => fail(s"wrong CellType $wrongCellType")
    }

    testLayer(new PyramidFactory("sentinel-2-l1c", "S2L1C", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10,10)), "sentinel2-L1C", date, Seq("B04", "B03", "B02"), testCellType)
  }

  @Test
  def testSentinel2L2A(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new PyramidFactory("sentinel-2-l2a", "S2L2A", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10,10)), "sentinel2-L2A", date, Seq("B08", "B04", "B03"))
  }

  @Test
  def testLandsat8(): Unit = {
    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 22), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    testLayer(new PyramidFactory("landsat-ot-l1", "LOTL1", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10,10)), "landsat8", date, Seq("B10", "B11"))
  }

  @Test
  def testLandsat8L2(): Unit = {
    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = LocalDate.of(2021, 4, 27).atStartOfDay(ZoneOffset.UTC)
    testLayer(new PyramidFactory("landsat-ot-l2", "landsat-ot-l2", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, sampleType = FLOAT32), "landsat8l2", date,
      Seq("B04", "B03", "B02"))
  }

  @Test
  def testDigitalNumbersOutput(): Unit = { // TODO: check output values programmatically
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    testLayer(new PyramidFactory("sentinel-2-l2a", "S2L2A", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10,10)), "sentinel2-L2A_mix", date, Seq("B04", "sunAzimuthAngles", "SCL"))
  }

  @Test
  def testUnknownBand(): Unit = {
    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 22), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    try
      testLayer(new PyramidFactory("landsat-ot-l1", datasetId = "LOTL1", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10,10)), "unknown", date, Seq("UNKNOWN"))
    catch {
      case e: SparkException =>
        assertTrue(e.getRootCause.getClass.toString, e.getRootCause.isInstanceOf[SentinelHubException])
    }
  }

  @Test
  def testInvalidClientSecret(): Unit = {
    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 22), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    try
      testLayer(new PyramidFactory("landsat-ot-l1", datasetId = "LOTL1", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), clientId, clientSecret = "???", maxSpatialResolution = CellSize(10,10)), "unknown", date, Seq("B10", "B11"))
    catch {
      case e: Exception =>
        assertTrue(e.getRootCause.getClass.toString, e.getRootCause.isInstanceOf[SentinelHubException])
    }
  }

  private def testLayer(pyramidFactory: PyramidFactory, layer: String, date: ZonedDateTime, bandNames: Seq[String],
                        test: MultibandTileLayerRDD[SpaceTimeKey] => Unit = _ => ()): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    val sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)

    try {
      val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

      val isoDate = ISO_OFFSET_DATE_TIME format date
      val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs, isoDate, isoDate, bandNames.asJava,
        metadata_properties = Collections.emptyMap[String, Any]) // https://github.com/scala/bug/issues/8911

      val (zoom, baseLayer) = pyramid
        .maxBy { case (zoom, _) => zoom }

      baseLayer.cache()

      println(s"got ${baseLayer.count()} tiles")

      test(baseLayer)

      val Raster(multibandTile, extent) = baseLayer
        .toSpatial()
        .crop(boundingBox.reproject(baseLayer.metadata.crs).extent)
        .stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs)
      tif.write(s"/tmp/${layer}_${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format date}.tif")
    } finally {
      sc.stop()
    }
  }

  @Test
  def testUtm(): Unit = {
    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)
      val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

      val utmBoundingBox = {
        val center = boundingBox.extent.center
        val utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)
        ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)
      }

      val endpoint = "https://services.sentinel-hub.com"
      val pyramidFactory = new PyramidFactory("sentinel-2-l2a", "S2L2A", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10,10))

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmBoundingBox.crs,
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B08", "B04", "B03").asJava,
        metadata_properties = Collections.emptyMap[String, Any]
      )

      val spatialLayer = layer
        .toSpatial()
        .crop(utmBoundingBox.extent)
        .cache()

      val Raster(multibandTile, extent) = spatialLayer.stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs)
      tif.write(s"/tmp/utm.tif")
    } finally sc.stop()
  }

  @Test
  def testCatalogApiLimitsProcessApiCalls(): Unit = {
    // [2021-02-03, 2021-02-06] has no data for orbit direction DESCENDING, only 2021-02-07 does
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val from = LocalDate.of(2021, 2, 3).atStartOfDay(ZoneOffset.UTC)
    val to = LocalDate.of(2021, 2, 7).atStartOfDay(ZoneOffset.UTC)

    val endpoint = "https://services.sentinel-hub.com"

    implicit val sc: SparkContext = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      def assembleGeoTiff(collectionId: String, expectedDateTimesCount: Long, expectedGetTileCount: Long): MultibandGeoTiff = {
        val catalogApiSpy = new CatalogApiSpy(endpoint)
        val processApiSpy = new ProcessApiSpy(endpoint)

        val pyramidFactory = new PyramidFactory(collectionId, "S1GRD", catalogApiSpy, processApiSpy, clientId,
          clientSecret, sampleType = FLOAT32, maxSpatialResolution = CellSize(10,10))

        val pyramid = pyramidFactory.pyramid_seq(
          boundingBox.extent,
          bbox_srs = s"EPSG:${boundingBox.crs.epsgCode.get}",
          ISO_OFFSET_DATE_TIME format from,
          ISO_OFFSET_DATE_TIME format to,
          band_names = Seq("VH", "VV").asJava,
          metadata_properties = Collections.singletonMap("orbitDirection", "DESCENDING")
        )

        val (_, baseLayer) = pyramid
          .maxBy { case (zoom, _) => zoom }

        val spatialLayer = baseLayer
          .toSpatial(to)
          .crop(boundingBox.reproject(baseLayer.metadata.crs))
          .cache()

        val Raster(multibandTile, extent) = spatialLayer.stitch()

        val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs)
        // tif.write(s"/tmp/testCatalogApiLimitsProcessApiCalls_collectionId_$collectionId.tif")

        assertEquals(expectedDateTimesCount, catalogApiSpy.dateTimesCount)
        assertEquals(expectedGetTileCount, processApiSpy.getTileCount)

        tif
      }

      val geoTiffWithoutCatalog =
        assembleGeoTiff(collectionId = null, expectedDateTimesCount = 0, expectedGetTileCount = 900)

      val geoTiffWithCatalog =
        assembleGeoTiff(collectionId = "sentinel-1-grd", expectedDateTimesCount = 1, expectedGetTileCount = 180)

      assertEquals(geoTiffWithoutCatalog, geoTiffWithCatalog)
    } finally sc.stop()
  }
}
