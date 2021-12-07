package org.openeo.geotrellissentinelhub

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.proj4.util.UTM
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff}
import geotrellis.raster.{CellSize, HasNoData, MultibandTile, Raster}
import geotrellis.spark._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{Ignore, Test}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.geotrellissentinelhub.SampleType.{FLOAT32, SampleType, UINT16}

import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.Deflater.BEST_COMPRESSION
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

  private val geoTiffOptions = GeoTiffOptions(DeflateCompression(BEST_COMPRESSION))

  @Test
  def testGamma0(): Unit = {
    val expected = referenceRaster("gamma0_catalog_14_2019-10-10.tif")

    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 10, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    def testCellType(baseLayer: MultibandTileLayerRDD[SpaceTimeKey]): Unit = baseLayer.metadata.cellType match {
      case cellType: HasNoData[Double @unchecked] if cellType.isFloatingPoint && cellType.noDataValue == 0.0 =>
      case wrongCellType => fail(s"wrong CellType $wrongCellType")
    }

    val actual = testLayer(new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, sampleType = FLOAT32,
      maxSpatialResolution = CellSize(10, 10)), "gamma0_catalog", date, Seq("VV", "VH", "dataMask"), testCellType)

    assertEquals(expected, actual)
  }

  @Test
  def testSentinel2L1C(): Unit = {
    val expected = referenceRaster("sentinel2-L1C_14_2019-09-21.tif")

    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    def testCellType(baseLayer: MultibandTileLayerRDD[SpaceTimeKey]): Unit = baseLayer.metadata.cellType match {
      case cellType: HasNoData[Int @unchecked] if !cellType.isFloatingPoint && cellType.noDataValue == 0 =>
      case wrongCellType => fail(s"wrong CellType $wrongCellType")
    }

    val actual = testLayer(new PyramidFactory("sentinel-2-l1c", "S2L1C", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10, 10)),
      "sentinel2-L1C", date, Seq("B04", "B03", "B02"), testCellType)

    assertEquals(expected, actual)
  }

  @Test
  def testSentinel2L2A(): Unit = {
    val expected = referenceRaster("sentinel2-L2A_14_2019-09-21.tif")

    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val actual = testLayer(new PyramidFactory("sentinel-2-l2a", "S2L2A", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10, 10)),
      "sentinel2-L2A", date, Seq("B08", "B04", "B03"))

    assertEquals(expected, actual)
  }

  @Test
  def testLandsat8(): Unit = {
    val expected = referenceRaster("landsat8_14_2019-09-22.tif")

    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 22), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val actual = testLayer(new PyramidFactory("landsat-ot-l1", "landsat-ot-l1", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10, 10)), "landsat8",
      date, Seq("B10", "B11"))

    assertEquals(expected, actual)
  }

  @Test
  def testLandsat8L2(): Unit = {
    val expected = referenceRaster("landsat8l2_14_2021-04-27.tif")

    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = LocalDate.of(2021, 4, 27).atStartOfDay(ZoneOffset.UTC)
    val actual = testLayer(new PyramidFactory("landsat-ot-l2", "landsat-ot-l2", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, sampleType = FLOAT32), "landsat8l2", date,
      Seq("B04", "B03", "B02"))

    assertEquals(expected, actual)
  }

  @Test
  def testMixedSentinel2L2A(): Unit = {
    val expected = referenceRaster("sentinel2-L2A_mix_14_2019-09-21.tif")

    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val actual = testLayer(new PyramidFactory("sentinel-2-l2a", "S2L2A", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10, 10)),
      "sentinel2-L2A_mix", date, Seq("B04", "sunAzimuthAngles", "SCL"))

    assertEquals(expected, actual)
  }

  @Test
  def testSclDilationCloudMasking(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val pyramidFactory = PyramidFactory.withoutGuardedRateLimiting(endpoint, "sentinel-2-l2a", "sentinel-2-l2a",
      clientId, clientSecret, processingOptions = util.Collections.emptyMap[String, Any], sampleType = UINT16,
      maxSpatialResolution = CellSize(10, 10))

    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      val boundingBox =
        ProjectedExtent(Extent(471275.3098352262, 5657503.248379398, 492660.20213888795, 5674436.759279663),
          CRS.fromEpsgCode(32631))

      val dataCubeParameters = new DataCubeParameters
      dataCubeParameters.maskingStrategyParameters = util.Collections.singletonMap("method", "mask_scl_dilation")

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B08", "B04", "B02", "SCL").asJava,
        metadata_properties = Collections.emptyMap[String, Any],
        dataCubeParameters
      )

      val spatialLayer = layer
        .toSpatial()
        .cache()

      val Raster(multibandTile, extent) = spatialLayer
        .crop(boundingBox.extent)
        .stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/scl_dilation_no_cloud_masking.tif")
    } finally sc.stop()
  }

  @Test
  def testUnknownBand(): Unit = {
    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 22), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    try
      testLayer(new PyramidFactory("landsat-ot-l1", datasetId = "landsat-ot-l1", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10, 10)), "unknown",
        date, Seq("UNKNOWN"))
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
      testLayer(new PyramidFactory("landsat-ot-l1", datasetId = "landsat-ot-l1", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), clientId, clientSecret = "???", maxSpatialResolution = CellSize(10, 10)),
        "unknown", date, Seq("B10", "B11"))
    catch {
      case e: Exception =>
        assertTrue(e.getRootCause.getClass.toString, e.getRootCause.isInstanceOf[SentinelHubException])
    }
  }

  private def testLayer(pyramidFactory: PyramidFactory, layer: String, date: ZonedDateTime, bandNames: Seq[String],
                        test: MultibandTileLayerRDD[SpaceTimeKey] => Unit = _ => ()): Raster[MultibandTile] = {
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

      val raster @ Raster(multibandTile, extent) = baseLayer
        .toSpatial()
        .crop(boundingBox.reproject(baseLayer.metadata.crs).extent)
        .stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/${layer}_${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format date}.tif")

      raster
    } finally sc.stop()
  }

  private def referenceRaster(name: String): Raster[MultibandTile] =
    MultibandGeoTiff(s"/data/projects/OpenEO/automated_test_files/$name").raster.mapTile(_.toArrayTile())

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

      val actual @ Raster(multibandTile, extent) = spatialLayer.stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/utm.tif")

      val expected = referenceRaster("utm.tif")

      assertEquals(expected, actual)
    } finally sc.stop()
  }

  @Test
  def testPolarizationDataFilter(): Unit = {
    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      val boundingBox = ProjectedExtent(Extent(488960.0, 6159880.0, 491520.0, 6162440.0), CRS.fromEpsgCode(32632))
      val date = ZonedDateTime.of(LocalDate.of(2016, 11, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)

      val endpoint = "https://services.sentinel-hub.com"
      val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), clientId, clientSecret, maxSpatialResolution = CellSize(10,10),
        sampleType = FLOAT32)

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("HV", "HH").asJava,
        metadata_properties = Collections.singletonMap("polarization", "DH")
      )

      val spatialLayer = layer
        .toSpatial()
        .crop(boundingBox.extent)
        .cache()

      val actual @ Raster(multibandTile, extent) = spatialLayer.stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/polarization.tif")

      val expected = referenceRaster("polarization.tif")

      assertEquals(expected, actual)
    } finally sc.stop()
  }

  @Ignore("the actual collection ID is a secret")
  @Test
  def testPlanetScope(): Unit = {
    import scala.io.Source

    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      val boundingBox = {
        val crs = CRS.fromEpsgCode(32628)

        ProjectedExtent(
          Extent(-17.25725769996643, 14.753334453316254, -17.255959510803223, 14.754506838531325)
            .reproject(LatLng, crs), crs
        )
      }

      val date = ZonedDateTime.of(LocalDate.of(2017, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)

      val planetCollectionId = {
        val in = Source.fromFile("/tmp/planetscope")

        try in.mkString.trim
        finally in.close()
      }

      val endpoint = "https://services.sentinel-hub.com"
      val pyramidFactory = new PyramidFactory(collectionId = planetCollectionId, datasetId = planetCollectionId,
        new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint), clientId, clientSecret,
        maxSpatialResolution = CellSize(3, 3), sampleType = FLOAT32)

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B3", "B2", "B1").asJava,
        metadata_properties = Collections.emptyMap[String, Any]
      )

      val spatialLayer = layer
        .toSpatial()
        .cache()

      val Raster(multibandTile, extent) = spatialLayer
        .stitch()
        .crop(boundingBox.extent) // it's jumping around again

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/testPlanetScope.tif")
    } finally sc.stop()
  }

  @Test
  def testCatalogApiLimitsProcessApiCalls(): Unit = {
    val expected = referenceRaster("testCatalogApiLimitsProcessApiCalls_collectionId_sentinel-1-grd.tif")

    // [2021-02-03, 2021-02-06] has no data for orbit direction DESCENDING, only 2021-02-07 does
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val from = LocalDate.of(2021, 2, 3).atStartOfDay(ZoneOffset.UTC)
    val to = LocalDate.of(2021, 2, 7).atStartOfDay(ZoneOffset.UTC)

    val endpoint = "https://services.sentinel-hub.com"

    implicit val sc: SparkContext = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      def assembleGeoTiff(collectionId: String, expectedDateTimesCount: Long,
                          expectedGetTileCount: Long): Raster[MultibandTile] = {
        val catalogApiSpy = new CatalogApiSpy(endpoint)
        val processApiSpy = new ProcessApiSpy(endpoint)

        val pyramidFactory = new PyramidFactory(collectionId, "sentinel-1-grd", catalogApiSpy, processApiSpy, clientId,
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

        val raster @ Raster(multibandTile, extent) = spatialLayer.stitch()

        val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs, geoTiffOptions)
        tif.write(s"/tmp/testCatalogApiLimitsProcessApiCalls_collectionId_$collectionId.tif")

        assertEquals(expectedDateTimesCount, catalogApiSpy.dateTimesCount)
        assertEquals(expectedGetTileCount, processApiSpy.getTileCount)

        raster
      }

      val rasterWithoutCatalog =
        assembleGeoTiff(collectionId = null, expectedDateTimesCount = 0, expectedGetTileCount = 900)

      val rasterWithCatalog =
        assembleGeoTiff(collectionId = "sentinel-1-grd", expectedDateTimesCount = 1, expectedGetTileCount = 180)

      assertEquals(rasterWithoutCatalog, rasterWithCatalog)
      assertEquals(expected, rasterWithoutCatalog)
    } finally sc.stop()
  }
}
