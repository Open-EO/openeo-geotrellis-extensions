package org.openeo.geotrellissentinelhub

import geotrellis.layer.SpaceTimeKey
import geotrellis.proj4.util.UTM
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff}
import geotrellis.raster.{CellSize, HasNoData, MultibandTile, Raster}
import geotrellis.shapefile.ShapeFileReader
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import geotrellis.vector.io.json.GeoJson
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.hamcrest.{CustomMatcher, Matcher}
import org.junit.Assert.{assertEquals, assertThat, assertTrue, fail}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.openeo.geotrelliscommon.BatchJobMetadataTracker.{ProductIdAndUrl, SH_FAILED_TILE_REQUESTS, SH_PU}
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, DataCubeParameters, ScopedMetadataTracker, SparseSpaceTimePartitioner}
import org.openeo.geotrellissentinelhub.SampleType.{FLOAT32, SampleType}

import java.io.File
import java.net.URL
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.Deflater.BEST_COMPRESSION
import scala.annotation.meta.getter
import scala.collection.JavaConverters._
import scala.io.Source

object PyramidFactoryTest {
  private implicit var sc: SparkContext = _
  private def testClassScopeMetadataTracker = ScopedMetadataTracker(scope = getClass.getName)

  implicit class WithRootCause(e: Throwable) {
    def getRootCause: Throwable = if (e.getCause == null) e else e.getCause.getRootCause
  }

  private class CatalogApiSpy(endpoint: String) extends CatalogApi {
    private val catalogApi = new DefaultCatalogApi(endpoint)
    private val searchCounter = new AtomicLong

    override def dateTimes(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime,
                           to: ZonedDateTime, accessToken: String,
                           queryProperties: util.Map[String, util.Map[String, Any]]): Seq[ZonedDateTime] =
      catalogApi.dateTimes(collectionId, geometry, geometryCrs, from, to, accessToken, queryProperties)

    override def search(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime,
                              to: ZonedDateTime, accessToken: String,
                              queryProperties: util.Map[String, util.Map[String, Any]]):
    Map[String, Feature[Geometry, FeatureData]] = {
      searchCounter.incrementAndGet()
      catalogApi.search(collectionId, geometry, geometryCrs, from, to, accessToken, queryProperties)
    }

    def searchCount: Long = searchCounter.get()
  }

  private class ProcessApiSpy(endpoint: String)(implicit sc: SparkContext) extends ProcessApi with Serializable {
    private val processApi = new DefaultProcessApi(endpoint)
    private val getTileCounter = sc.longAccumulator

    override def getTile(datasetId: String, projectedExtent: ProjectedExtent, date: ZonedDateTime, width: Int,
                         height: Int, bandNames: Seq[String], sampleType: SampleType,
                         additionalDataFilters: util.Map[String, Any], processingOptions: util.Map[String, Any],
                         accessToken: String): (MultibandTile, Double) = {
      getTileCounter.add(1)
      processApi.getTile(datasetId, projectedExtent, date, width, height, bandNames, sampleType,
        additionalDataFilters, processingOptions, accessToken)
    }

    def getTileCount: Long = getTileCounter.value
  }

  @BeforeClass
  def setupSpark(): Unit = sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

  @AfterClass
  def tearDownSpark(): Unit = sc.stop()

  @BeforeClass def tracking(): Unit ={
    BatchJobMetadataTracker.setGlobalTracking(true)
  }

  @AfterClass def trackingOff(): Unit ={
    BatchJobMetadataTracker.setGlobalTracking(false)
  }

  @AfterClass
  def printProcessingUnitsUsed(): Unit =
    println(s"$testClassScopeMetadataTracker consumed a total of ${testClassScopeMetadataTracker.sentinelHubProcessingUnits} PUs")

  private class SingleResultCaptor[R] extends Answer[R] {
    var result: R = _

    override def answer(invocation: InvocationOnMock): R = {
      result = invocation.callRealMethod().asInstanceOf[R]
      result
    }
  }
}

class PyramidFactoryTest {
  import PyramidFactoryTest._

  @(Rule@getter)
  val temporaryFolder = new TemporaryFolder

  private val clientId = Utils.clientId
  private val clientSecret = Utils.clientSecret
  private val authorizer = new MemoizedAuthApiAccessTokenAuthorizer(
    clientId, clientSecret)

  private val geoTiffOptions = GeoTiffOptions(DeflateCompression(BEST_COMPRESSION))

  @Before
  def clearTracker(): Unit = {
    BatchJobMetadataTracker.clearGlobalTracker()
  }

  @Test
  def testGamma0(): Unit = {
    val expected = referenceRaster("gamma0_catalog_14_2019-10-10.tif")

    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 10, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    def testCellType(baseLayer: MultibandTileLayerRDD[SpaceTimeKey]): Unit = baseLayer.metadata.cellType match {
      case cellType: HasNoData[Float @unchecked] if cellType.isFloatingPoint && cellType.noDataValue == 0.0 =>
      case wrongCellType => fail(s"wrong CellType $wrongCellType")
    }

    val actual = testLayer(new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, sampleType = FLOAT32,
      maxSpatialResolution = CellSize(10, 10)), "gamma0_catalog", date, Seq("VV", "VH", "dataMask"), testCellType)

    assertEquals(expected, actual)
  }

  @Test
  def testSentinel2L1C(): Unit = {
    val expected = referenceRaster("sentinel2-L1C_14_2019-09-21.tif")

    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    def testCellType(baseLayer: MultibandTileLayerRDD[SpaceTimeKey]): Unit = baseLayer.metadata.cellType match {
      case cellType: HasNoData[Short @unchecked] if !cellType.isFloatingPoint && cellType.noDataValue == 0 =>
      case wrongCellType => fail(s"wrong CellType $wrongCellType")
    }

    val actual = testLayer(new PyramidFactory("sentinel-2-l1c", "S2L1C", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10, 10)),
      "sentinel2-L1C", date, Seq("B04", "B03", "B02"), testCellType)

    assertEquals(expected, actual)
  }

  @Test
  def testSentinel2L2A(): Unit = {
    val expected = referenceRaster("sentinel2-L2A_14_2019-09-21.tif")

    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val actual = testLayer(new PyramidFactory("sentinel-2-l2a", "S2L2A", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10, 10)),
      "sentinel2-L2A", date, Seq("B08", "B04", "B03"))

    assertEquals(expected, actual)
    val pu = BatchJobMetadataTracker.tracker("").asDict().get(SH_PU).asInstanceOf[Double]

    assertEquals(45.0, pu, 0.001)
  }

  @Test
  def testLandsat8(): Unit = {
    val expected = referenceRaster("landsat8_14_2019-09-22.tif")

    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 22), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val actual = testLayer(new PyramidFactory("landsat-ot-l1", "landsat-ot-l1", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10, 10)), "landsat8",
      date, Seq("B10", "B11"))

    assertEquals(expected, actual)
  }

  @Test
  def testLandsat8L2(): Unit = {
    val expected = referenceRaster("landsat8l2_14_2021-04-27.tif")

    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = LocalDate.of(2021, 4, 27).atStartOfDay(ZoneOffset.UTC)
    val actual = testLayer(new PyramidFactory("landsat-ot-l2", "landsat-ot-l2", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, sampleType = FLOAT32), "landsat8l2", date,
      Seq("B04", "B03", "B02"))

    assertEquals(expected, actual)
  }

  @Test
  def testMixedSentinel2L2A(): Unit = {
    val expected = referenceRaster("sentinel2-L2A_mix_14_2019-09-21.tif")

    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val actual = testLayer(new PyramidFactory("sentinel-2-l2a", "S2L2A", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10, 10)),
      "sentinel2-L2A_mix", date, Seq("B04", "sunAzimuthAngles", "SCL"))

    assertEquals(expected, actual)
  }

  @Test
  def testSclDilationCloudMasking(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val pyramidFactory = new PyramidFactory("sentinel-2-l2a", "sentinel-2-l2a", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer)

    val boundingBox =
      ProjectedExtent(Extent(471275.3098352262, 5657503.248379398, 492660.20213888795, 5674436.759279663),
        CRS.fromEpsgCode(32631))

    val dataCubeParameters = new DataCubeParameters
    dataCubeParameters.maskingStrategyParameters = new util.HashMap()
    dataCubeParameters.maskingStrategyParameters.put("method", "mask_scl_dilation")
    dataCubeParameters.maskingStrategyParameters.put("erosion_kernel_size", 1.asInstanceOf[Object])

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("B08", "B04",  "SCL").asJava,
      metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
      dataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    val spatialLayer = layer
      .toSpatial()
      .cache()

    val Raster(multibandTile, extent) = spatialLayer
      .crop(boundingBox.extent)
      .stitch()

    val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/scl_dilation_no_cloud_masking.tif")
  }

  @Test
  def testUnknownBand(): Unit = {
    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 22), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    try
      testLayer(new PyramidFactory("landsat-ot-l1", datasetId = "landsat-ot-l1", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10, 10)), "unknown",
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
        new DefaultProcessApi(endpoint), new MemoizedAuthApiAccessTokenAuthorizer(clientId, clientSecret = "???")),
        "unknown", date, Seq("B10", "B11"))
    catch {
      case e: Exception =>
        assertTrue(e.getRootCause.getClass.toString, e.getRootCause.isInstanceOf[SentinelHubException])
    }
  }

  private def testLayer(pyramidFactory: PyramidFactory, layer: String, datetime: ZonedDateTime, bandNames: Seq[String],
                        test: MultibandTileLayerRDD[SpaceTimeKey] => Unit = _ => ()): Raster[MultibandTile] = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress", "true")

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

    val isoFrom = ISO_OFFSET_DATE_TIME format datetime
    val isoUntil = ISO_OFFSET_DATE_TIME format (datetime plusDays 1)

    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs, isoFrom, isoUntil, bandNames.asJava,
      metadata_properties = Collections.emptyMap[String, util.Map[String, Any]], // https://github.com/scala/bug/issues/8911
      correlationId = testClassScopeMetadataTracker.scope,
    )

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
    tif.write(s"/tmp/${layer}_${zoom}_${DateTimeFormatter.ISO_LOCAL_DATE format datetime}.tif")

    raster
  }

  private def referenceRaster(name: String): Raster[MultibandTile] =
    MultibandGeoTiff(s"/data/projects/OpenEO/automated_test_files/$name").raster.mapTile(_.toArrayTile())

  @Test
  def testUtm(): Unit = {
    val from = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val until = from plusDays 1
    testUtm(from, until)
  }

  @Test
  def testUtmSameFromUntil(): Unit = {
    // asserts original behaviour where end timestamp is considered inclusive
    val from = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val until = from
    testUtm(from, until)
  }

  // TODO: use parameterized test
  private def testUtm(from: ZonedDateTime, until: ZonedDateTime): Unit = {
    val expected = referenceRaster("utm.tif")

    val testScopeMetadataTracker = ScopedMetadataTracker(scope = "testUtm")

    try {
      val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

      val utmBoundingBox = {
        val center = boundingBox.extent.center
        val utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)
        ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)
      }

      val endpoint = "https://services.sentinel-hub.com"
      val pyramidFactory = new PyramidFactory("sentinel-2-l2a", "S2L2A", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10,10))

      val dataCubeParameters = new DataCubeParameters()
      dataCubeParameters.layoutScheme = "FloatingLayoutScheme"
      dataCubeParameters.globalExtent = Some(utmBoundingBox)

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmBoundingBox.crs,
        from_datetime = ISO_OFFSET_DATE_TIME format from,
        until_datetime = ISO_OFFSET_DATE_TIME format until,
        band_names = Seq("B08", "B04", "B03").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
        dataCubeParameters,
        correlationId = testScopeMetadataTracker.scope,
      )

      assertTrue(layer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])

      val spatialLayer = layer
        .toSpatial().withContext{_.map(t=> {
          assert(t._1.row>=0 && t._1.col>=0)
          t
        } )}
        .crop(utmBoundingBox.extent)
        .cache()

      val actual @ Raster(multibandTile, extent) = spatialLayer.stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/utm.tif")

      assertEquals(expected, actual)

      val trackedMetadata = BatchJobMetadataTracker.tracker("").asDict()
      val numFailedRequests = trackedMetadata.get(SH_FAILED_TILE_REQUESTS).asInstanceOf[Long]

      assertTrue(s"unexpected number of failed tile requests: $numFailedRequests", numFailedRequests >= 0)

      assertTrue(s"PUs: ${testScopeMetadataTracker.sentinelHubProcessingUnits}",
        testScopeMetadataTracker.sentinelHubProcessingUnits > 0)
    } finally {
      testClassScopeMetadataTracker.addSentinelHubProcessingUnits(testScopeMetadataTracker.sentinelHubProcessingUnits)
      ScopedMetadataTracker.remove(testScopeMetadataTracker.scope)
    }
  }

  @Test
  def testUtmSparse(): Unit = {
    // small (1 tile request) regions in the upper left and lower right corners of [2.59003, 51.069, 2.8949, 51.2206]
    val size = 0.001
    val upperLeftBoundingBox =
      Extent(xmin = 2.6, ymin = 51.219034670299344, xmax = 2.6 + size, ymax = 51.22005609157961)
    val lowerRightBoundingBox =
      Extent(xmin = 2.888975143432617, ymin = 51.085, xmax = 2.8932666778564453, ymax = 51.085 + size)

    val polygons = Array(upperLeftBoundingBox, lowerRightBoundingBox)
      .map(extent => MultiPolygon(extent.toPolygon()))

    val date = ZonedDateTime.of(LocalDate.of(2019, 11, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val (utmPolygons, utmCrs) = {
      val center = GeometryCollection(polygons).extent.center
      val utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)
      (polygons.map(_.reproject(LatLng, utmCrs)), utmCrs)
    }

    val endpoint = "https://services.sentinel-hub.com"
    val processApiSpy = new ProcessApiSpy(endpoint)

    val pyramidFactory = new PyramidFactory("sentinel-2-l2a", "sentinel-2-l2a", new DefaultCatalogApi(endpoint),
      processApiSpy, authorizer, maxSpatialResolution = CellSize(10, 10))

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      utmPolygons, utmCrs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("B08", "B04", "B03").asJava,
      metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    assertTrue(layer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])

    val sentinel1Layer: MultibandTileLayerRDD[SpaceTimeKey] = sparseSentinel1Layer(utmPolygons,utmCrs, date)
    assertTrue(sentinel1Layer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])
    val s2Part = layer.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]]
    val s1Part = sentinel1Layer.partitioner.get.asInstanceOf[SpacePartitioner[SpaceTimeKey]]

    print(s2Part)
    print(s1Part)
    assertEquals(s2Part.bounds,s1Part.bounds)
    assertTrue(s2Part.index.isInstanceOf[SparseSpaceTimePartitioner])
    assertTrue(s1Part.index.isInstanceOf[SparseSpaceTimePartitioner])
    assertEquals(s2Part.index,s1Part.index)

    val spatialLayer = layer
      .toSpatial().withContext {
      _.map(t => {
        assert(t._1.row >= 0 && t._1.col >= 0)
        t
      })
    }
      .cache()

    val utmPolygonsExtent = utmPolygons.toSeq.extent
    val Some(Raster(multibandTile, extent)) = spatialLayer
      .sparseStitch(utmPolygonsExtent)
      .map(_.crop(utmPolygonsExtent))

    val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/utm_sparse.tif")

    assertEquals(2, processApiSpy.getTileCount)
    // TODO: compare these regions against those for the non-sparse case (testUtm)
  }

  @Test
  def testOverlappingPolygons(): Unit = {
    val upperLeftPolygon =
      Extent(4.093673229217529, 50.39570215730746, 4.095818996429443, 50.39704266811707).toPolygon()
    val lowerRightPolygon =
      Extent(4.094831943511963, 50.39508660393027, 4.0970635414123535, 50.396317702692095).toPolygon()

    assertTrue("polygons do not overlap", upperLeftPolygon intersects lowerRightPolygon)

    val polygons: Array[MultiPolygon] = Array(
      MultiPolygon(upperLeftPolygon),
      MultiPolygon(lowerRightPolygon)
    )

    val date = ZonedDateTime.of(LocalDate.of(2021, 4, 3), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val (utmPolygons, utmCrs) = {
      val center = GeometryCollection(polygons).extent.center
      val utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)
      (polygons.map(_.reproject(LatLng, utmCrs)), utmCrs)
    }

    val endpoint = "https://services.sentinel-hub.com"

    val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10, 10),
      sampleType = FLOAT32)

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      utmPolygons, utmCrs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("VH", "VV").asJava,
      metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    val spatialLayer = layer
      .toSpatial().withContext {
      _.map(t => {
        assert(t._1.row >= 0 && t._1.col >= 0)
        t
      })
    }
      .cache()

    val utmPolygonsExtent = utmPolygons.toSeq.extent
    val Some(Raster(multibandTile, extent)) = spatialLayer
      .sparseStitch(utmPolygonsExtent)
      .map(_.crop(utmPolygonsExtent))

    val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/overlapping.tif")
  }

  @Test
  def testPolarizationDataFilter(): Unit = {
    val expected = referenceRaster("polarization.tif")

    val boundingBox = ProjectedExtent(Extent(488960.0, 6159880.0, 491520.0, 6162440.0), CRS.fromEpsgCode(32632))
    val date = ZonedDateTime.of(LocalDate.of(2016, 11, 10), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val polygon = MultiPolygon(boundingBox.extent.toPolygon())
    val layer = sparseSentinel1Layer(Array(polygon), boundingBox.crs, date)

    val spatialLayer = layer
      .toSpatial()
      .crop(boundingBox.extent)
      .cache()

    val actual @ Raster(multibandTile, extent) = spatialLayer.stitch()

    val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/polarization.tif")

    assertEquals(expected, actual)
  }

  private def sparseSentinel1Layer(polygon: Array[MultiPolygon], crs:CRS, date: ZonedDateTime): MultibandTileLayerRDD[SpaceTimeKey] = {
    val endpoint = "https://services.sentinel-hub.com"
    val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10, 10),
      sampleType = FLOAT32)

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      polygon, crs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("HV", "HH").asJava,
      metadata_properties = util.Collections.emptyMap[String, util.Map[String, Any]],
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )
    layer
  }

  @Ignore("the actual collection ID is a secret")
  @Test
  def testPlanetScope(): Unit = {
    val planetCollectionId = {
      val in = Source.fromFile("/tmp/african_script_contest_collection_id")

      try in.mkString.trim
      finally in.close()
    }

    val utmBoundingBox = {
      val boundingBox = ProjectedExtent(
        Extent(-17.25725769996643, 14.753334453316254, -17.255959510803223, 14.754506838531325), LatLng)
      val center = boundingBox.extent.center
      val utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)

      ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)
    }

    val date = ZonedDateTime.of(LocalDate.of(2017, 1, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val endpoint = "https://services.sentinel-hub.com"
    val pyramidFactory = new PyramidFactory(collectionId = planetCollectionId, datasetId = planetCollectionId,
      new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint), authorizer,
      maxSpatialResolution = CellSize(3, 3), sampleType = FLOAT32)

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmBoundingBox.crs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("B3", "B2", "B1").asJava,
      metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    val spatialLayer = layer
      .toSpatial()
      .cache()

    val Raster(multibandTile, extent) = spatialLayer
      .stitch()
      .crop(utmBoundingBox.extent) // it's jumping around again

    val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/testPlanetScope.tif")
  }

  @Ignore("the actual collection ID is a secret")
  @Test
  def testPlanetScopeCatalogReturnsMultiPolygonFeatures(): Unit = {
    val planetCollectionId = {
      val in = Source.fromFile("/tmp/african_script_contest_collection_id")

      try in.mkString.trim
      finally in.close()
    }

    val utmBoundingBox = {
      val boundingBox = ProjectedExtent(Extent(-17.348314, 14.743654, -17.307115, 14.762578), LatLng)
      val center = boundingBox.extent.center
      val utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)

      ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)
    }

    val date = ZonedDateTime.of(LocalDate.of(2021, 5, 11), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val endpoint = "https://services.sentinel-hub.com"
    val pyramidFactory = new PyramidFactory(collectionId = planetCollectionId, datasetId = planetCollectionId,
      new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint), authorizer,
      maxSpatialResolution = CellSize(3, 3), sampleType = FLOAT32)

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmBoundingBox.crs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("B2").asJava,
      metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    val spatialLayer = layer
      .toSpatial()
      .cache()

    val Raster(multibandTile, extent) = spatialLayer
      .stitch()
      .crop(utmBoundingBox.extent) // it's jumping around again

    val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/testPlanetScopeCatalogReturnsMultiPolygonFeatures.tif")
  }

  @Ignore("the actual collection ID is a secret")
  @Test
  def testPlanetScopeCatalogReturnsPolygonFeatures(): Unit = {
    val planetCollectionId = {
      val in = Source.fromFile("/tmp/uc8_collection_id")

      try in.mkString.trim
      finally in.close()
    }

    val utmBoundingBox = {
      val boundingBox = ProjectedExtent(Extent(8.51666, 49.86128, 8.52365, 49.86551), LatLng)
      val center = boundingBox.extent.center
      val utmCrs = UTM.getZoneCrs(lon = center.getX, lat = center.getY)

      ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)
    }

    val date = ZonedDateTime.of(LocalDate.of(2021, 12, 6), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val endpoint = "https://services.sentinel-hub.com"
    val pyramidFactory = new PyramidFactory(collectionId = planetCollectionId, datasetId = planetCollectionId,
      new DefaultCatalogApi(endpoint), new DefaultProcessApi(endpoint), authorizer,
      maxSpatialResolution = CellSize(3, 3), sampleType = FLOAT32)

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmBoundingBox.crs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("B4").asJava,
      metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    val spatialLayer = layer
      .toSpatial()
      .cache()

    val Raster(multibandTile, extent) = spatialLayer
      .stitch()
      .crop(utmBoundingBox.extent) // it's jumping around again

    val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/testPlanetScopeCatalogReturnsPolygonFeatures.tif")
  }

  @Test
  def testEoCloudCover(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val pyramidFactory = new PyramidFactory("sentinel-2-l2a", "sentinel-2-l2a", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer)

    val boundingBox =
      ProjectedExtent(Extent(471275.3098352262, 5657503.248379398, 492660.20213888795, 5674436.759279663),
        CRS.fromEpsgCode(32631))

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("B04", "B03", "B02").asJava,
      metadata_properties = Collections.singletonMap("eo:cloud_cover", Collections.singletonMap("lte", 20)),
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    val spatialLayer = layer
      .toSpatial()
      .cache()

    val Raster(multibandTile, extent) = spatialLayer
      .crop(boundingBox.extent)
      .stitch()

    val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/testEoCloudCover.tif")

    // TODO: add assertions
  }

  @Test
  def testFilterByTileIds(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2024, 4, 24), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val expectedTileIds = Set("31UES", "31UFS")

    val pyramidFactory = new PyramidFactory("sentinel-2-l2a", "sentinel-2-l2a", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer)

    val utmBoundingBox = {
      val boundingBox = ProjectedExtent(Extent(
        4.4158740490713804, 51.4204485519121945,
        4.4613941769140322, 51.4639210615473885), LatLng)

      val utmCrs = UTM.getZoneCrs(boundingBox.extent.center.x, boundingBox.extent.center.y)
      ProjectedExtent(boundingBox.reproject(utmCrs), utmCrs)
    }

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmBoundingBox.crs,
      from_datetime = ISO_OFFSET_DATE_TIME format date,
      until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
      band_names = Seq("B04", "B03", "B02").asJava,
      metadata_properties = Collections.singletonMap("tileId",
        Collections.singletonMap("in", expectedTileIds.toBuffer.asJava)),
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    val spatialLayer = layer
      .toSpatial()
      .cache()

    val Raster(multibandTile, extent) = spatialLayer
      .crop(utmBoundingBox.extent)
      .stitch()

    val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
    tif.write(s"/tmp/testTileId.tif")

    val links = BatchJobMetadataTracker.tracker("").asDict()
      .get("links").asInstanceOf[util.HashMap[String, util.List[ProductIdAndUrl]]]

    val actualTileIds = links.get("sentinel-2-l2a").asScala
      .map(_.getId)
      .flatMap(Sentinel2L2a.extractTileId)
      .toSet

    assertEquals(expectedTileIds, actualTileIds)
  }

  @Test
  def testMapzenDem(): Unit = {
    val endpoint = "https://services-uswest2.sentinel-hub.com"

    // from https://collections.eurodatacube.com/stac/mapzen-dem.json
    val maxSpatialResolution = CellSize(0.000277777777778, 0.000277777777778)

    def assembleGeoTiff(numberOfDays: Int): Raster[MultibandTile] = {
      val from = LocalDate.of(2023, 9, 12).atStartOfDay(ZoneOffset.UTC)
      val until = from plusDays numberOfDays // excludes upper

      val catalogApiSpy = new CatalogApiSpy(endpoint)
      val processApiSpy = new ProcessApiSpy(endpoint)

      val pyramidFactory = new PyramidFactory(collectionId = null, datasetId = "dem", catalogApiSpy,
        processApiSpy, authorizer, sampleType = FLOAT32, maxSpatialResolution = maxSpatialResolution)

      val boundingBox = ProjectedExtent(Extent(2.59003, 51.069, 2.8949, 51.2206), LatLng)

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
        from_datetime = ISO_OFFSET_DATE_TIME format from,
        until_datetime = ISO_OFFSET_DATE_TIME format until,
        band_names = Seq("DEM").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
        dataCubeParameters = new DataCubeParameters,
        correlationId = testClassScopeMetadataTracker.scope,
      )

      layer.cache()

      val distinctDates = layer
        .keys
        .map(_.time)
        .distinct()
        .sortBy(identity)
        .collect()

      assertEquals(sequentialDays(from, from plusDays (numberOfDays - 1)), distinctDates.toSeq) // includes upper

      val spatialLayer = layer
        .toSpatial(from.toLocalDate.atStartOfDay(ZoneOffset.UTC))

      val raster @ Raster(multibandTile, extent) = spatialLayer
        .crop(boundingBox.extent)
        .stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/testMapzenDem_${numberOfDays}_days.tif")

      assertEquals(0, catalogApiSpy.searchCount) // doesn't use a catalog
      assertEquals(15, processApiSpy.getTileCount) // independent of temporal extent

      raster
    }

    val firstRasterFromSmallNumberOfDays = assembleGeoTiff(numberOfDays = 1)
    val firstRasterFromLargeNumberOfDays = assembleGeoTiff(numberOfDays = 100)

    assertEquals(firstRasterFromSmallNumberOfDays, firstRasterFromLargeNumberOfDays)
  }

  @Test
  def testCatalogApiLimitsProcessApiCalls(): Unit = {
    val expected = referenceRaster("testCatalogApiLimitsProcessApiCalls_collectionId_sentinel-1-grd.tif")

    // [2021-02-03, 2021-02-06] has no data for orbit direction DESCENDING, only 2021-02-07 does
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val from = LocalDate.of(2021, 2, 3).atStartOfDay(ZoneOffset.UTC)
    val until = LocalDate.of(2021, 2, 8).atStartOfDay(ZoneOffset.UTC)

    val endpoint = "https://services.sentinel-hub.com"

    def assembleGeoTiff(collectionId: String, expectedSearchCount: Long,
                        expectedGetTileCount: Long): Raster[MultibandTile] = {
      val catalogApiSpy = new CatalogApiSpy(endpoint)
      val processApiSpy = new ProcessApiSpy(endpoint)

      val pyramidFactory = new PyramidFactory(collectionId, "sentinel-1-grd", catalogApiSpy, processApiSpy,
        authorizer, sampleType = FLOAT32)

      val pyramid = pyramidFactory.pyramid_seq(
        boundingBox.extent,
        bbox_srs = s"EPSG:${boundingBox.crs.epsgCode.get}",
        ISO_OFFSET_DATE_TIME format from,
        ISO_OFFSET_DATE_TIME format until,
        band_names = Seq("VH", "VV").asJava,
        metadata_properties = Collections.singletonMap("orbitDirection", Collections.singletonMap("eq", "DESCENDING")),
        correlationId = testClassScopeMetadataTracker.scope,
      )

      val (_, baseLayer) = pyramid
        .maxBy { case (zoom, _) => zoom }

      val spatialLayer = baseLayer
        .toSpatial(LocalDate.of(2021, 2, 7).atStartOfDay(ZoneOffset.UTC))
        .crop(boundingBox.reproject(baseLayer.metadata.crs))
        .cache()

      val raster @ Raster(multibandTile, extent) = spatialLayer.stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, baseLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/testCatalogApiLimitsProcessApiCalls_collectionId_$collectionId.tif")

      assertEquals(expectedSearchCount, catalogApiSpy.searchCount)
      assertEquals(expectedGetTileCount, processApiSpy.getTileCount)

      raster
    }

    val rasterWithoutCatalog =
      assembleGeoTiff(collectionId = null, expectedSearchCount = 0, expectedGetTileCount = 900)

    val rasterWithCatalog =
      assembleGeoTiff(collectionId = "sentinel-1-grd", expectedSearchCount = 1, expectedGetTileCount = 180)

    assertEquals(rasterWithoutCatalog, rasterWithCatalog)
    assertEquals(expected, rasterWithoutCatalog)

    val trackedMetadata = BatchJobMetadataTracker.tracker("").asDict()
    val numFailedRequests = trackedMetadata.get(SH_FAILED_TILE_REQUESTS).asInstanceOf[Long]

    assertTrue(s"unexpected number of failed tile requests: $numFailedRequests", numFailedRequests >= 0)
  }

  @Test
  def testSoftErrors(): Unit = {
    def provokeNonTransientError(softErrors: Boolean): Unit = {
      // any non-transient error will do but covering a failed tile request is most important
      val endpoint = "https://services.sentinel-hub.com"
      val processingOptions =
        Map("demInstance" -> "MAPZEN", "backCoeff" -> "GAMMA0_TERRAIN", "orthorectify" -> true).asJava

      val boundingBox =
        ProjectedExtent(Extent(488960.0, 6159880.0, 491520.0, 6162440.0), CRS.fromEpsgCode(32632))

      val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), authorizer, processingOptions, sampleType = FLOAT32,
        maxSoftErrorsRatio = if (softErrors) 1.0 else 0.0)

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
        polygons_crs = boundingBox.crs,
        from_datetime = "2016-11-10T00:00:00Z",
        until_datetime = "2016-11-11T00:00:00Z",
        band_names = util.Arrays.asList("VV"),
        metadata_properties = util.Collections.emptyMap(),
        dataCubeParameters = new DataCubeParameters,
        correlationId = testClassScopeMetadataTracker.scope,
      )

      layer.isEmpty() // force evaluation
    }

    // sanity check
    try {
      provokeNonTransientError(softErrors = false)
      fail("should have thrown a SentinelHubException")
    } catch {
      case e: SparkException =>
        val SentinelHubException(_, 400, _, responseBody) = e.getRootCause
        assertTrue(responseBody, responseBody contains "not present in Sentinel 1 tile")
    }

    provokeNonTransientError(softErrors = true) // shouldn't throw

    val trackedMetadata = BatchJobMetadataTracker.tracker("").asDict()
    val numFailedRequests = trackedMetadata.get(SH_FAILED_TILE_REQUESTS).asInstanceOf[Long]

    assertTrue(s"expected at least one failed tile request but got $numFailedRequests instead", numFailedRequests > 0)
  }

  @Test
  def testSentinel5PL2DuplicateRequestsDatacube_seq(): Unit = {
    def layerFromDatacube_seq(pyramidFactory: PyramidFactory, boundingBox: ProjectedExtent, date: ZonedDateTime,
                              bandNames: Seq[String], metadata_properties: util.Map[String, util.Map[String, Any]]):
    MultibandTileLayerRDD[SpaceTimeKey] = {

      val datacubeParams = new DataCubeParameters()
      // To avoid the tile edge to move along with the extent. One specetime key would cover half Europe
      datacubeParams.globalExtent = Some(ProjectedExtent(Extent(-30, 0, 30, 60), LatLng))

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
        from_datetime = ISO_OFFSET_DATE_TIME format date,
        until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
        band_names = bandNames.asJava,
        metadata_properties = metadata_properties,
        datacubeParams,
        correlationId = testClassScopeMetadataTracker.scope,
      )

      layer
    }

    // requires only a single tile request to cover the input extent
    val expectedGetTileCount = 1

    testSentinel5PL2DuplicateRequests(layerFromDatacube_seq, expectedGetTileCount,
      "/tmp/testSentinel5PL2DuplicateRequestsDatacube_seq.tif")
  }

  @Test
  def testSentinel5PL2DuplicateRequestsPyramid_seq(): Unit = {
    def layerFromPyramid_seq(pyramidFactory: PyramidFactory, boundingBox: ProjectedExtent, date: ZonedDateTime,
                             bandNames: Seq[String], metadata_properties: util.Map[String, util.Map[String, Any]]):
    MultibandTileLayerRDD[SpaceTimeKey] = {
      val (_, baseLayer) = pyramidFactory.pyramid_seq(boundingBox.extent, s"EPSG:${boundingBox.crs.epsgCode.get}",
        from_datetime = ISO_OFFSET_DATE_TIME format date,
        until_datetime = ISO_OFFSET_DATE_TIME format (date plusDays 1),
        band_names = bandNames.asJava,
        metadata_properties = metadata_properties,
        correlationId = testClassScopeMetadataTracker.scope,
      ).maxBy { case (zoom, _) => zoom }

      baseLayer
    }

    // max-zoom ZoomedLayoutScheme(WebMercator) requires 4 tile requests to cover the input extent
    val expectedGetTileCount = 4

    testSentinel5PL2DuplicateRequests(layerFromPyramid_seq, expectedGetTileCount,
      "/tmp/testSentinel5PL2DuplicateRequestsPyramid_seq.tif")
  }

  private def testSentinel5PL2DuplicateRequests(layerFromSeq: (PyramidFactory, ProjectedExtent, ZonedDateTime,
    Seq[String], util.Map[String, util.Map[String, Any]]) => MultibandTileLayerRDD[SpaceTimeKey],
                                                expectedGetTileCount: Int, outputTiff: String): Unit = {
    // mimics /home/bossie/Documents/VITO/applying mask increases PUs drastically #95/process_graph_without_mask_smaller.json
    val boundingBox = ProjectedExtent(Extent(xmin = 6.1, ymin = 46.16, xmax = 6.11, ymax = 46.17), LatLng)
    val date = ZonedDateTime.of(LocalDate.of(2018, 7, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val endpoint = "https://creodias.sentinel-hub.com"

    val catalogApiSpy = spy(new DefaultCatalogApi(endpoint))
    val processApiSpy = new ProcessApiSpy(endpoint)

    val catalogApiResultCaptor = new SingleResultCaptor[Map[String, Feature[Geometry, ZonedDateTime]]]

    doAnswer(catalogApiResultCaptor).when(catalogApiSpy)
      .search(any(), any(), any(), any(), any(), any(), any())

    val pyramidFactory = new PyramidFactory("sentinel-5p-l2", "sentinel-5p-l2", catalogApiSpy,
      processApiSpy, authorizer, maxSpatialResolution = CellSize(0.054563492063483, 0.034722222222216),
      sampleType = FLOAT32)

    val layer = layerFromSeq(pyramidFactory, boundingBox, date, Seq("NO2"),
      Collections.emptyMap[String, util.Map[String, Any]])

    verify(catalogApiSpy).search(eqTo("sentinel-5p-l2"), eqTo(boundingBox.extent.toPolygon()), eqTo(LatLng),
      from = eqTo(date), to = eqTo(ZonedDateTime.parse("2018-07-01T23:59:59.999999999Z")), accessToken = any(),
      queryProperties = eqTo(util.Collections.emptyMap()))

    def greaterThan(n: Int): Matcher[Int] = new CustomMatcher[Int](s"greater than $n") {
      override def matches(item: Any): Boolean = item match {
        case i: Int => i > n
      }
    }

    assertThat(catalogApiResultCaptor.result.toString(), catalogApiResultCaptor.result.size, greaterThan(10))

    for (Seq((leftId, Feature(leftGeom, _)), (rightId, Feature(rightGeom, _))) <- catalogApiResultCaptor.result.toSeq.sliding(2)) {
      assert(leftId != rightId)
      assertTrue(leftGeom intersects rightGeom)
    }

    val spatialLayer = layer
      .toSpatial()
      .cache()

    val raster = spatialLayer
      .stitch()
      .reproject(spatialLayer.metadata.crs, boundingBox.crs)
      .crop(boundingBox.extent)

    val tif = MultibandGeoTiff(raster.tile, raster.extent, boundingBox.crs, geoTiffOptions)
    tif.write(outputTiff)

    assertEquals(expectedGetTileCount, processApiSpy.getTileCount)
  }

  @Test
  def testLargeNumberOfInputPolygons(): Unit = {
    val tempDir = temporaryFolder.getRoot
    val geometriesFileStem = "Fields_to_extract_2021_30SVH_RAW_0"

    for (extension <- Seq("cpg", "dbf", "prj", "shp", "shx")) {
      val geometriesFilename = s"$geometriesFileStem.$extension"
      val geometriesUrl = new URL(s"https://artifactory.vgt.vito.be/artifactory/testdata-public/parcels/$geometriesFilename")
      FileUtils.copyURLToFile(geometriesUrl, new File(tempDir, geometriesFilename))
    }

    val endpoint = "https://services.sentinel-hub.com"

    val geometriesFile = new File(tempDir, s"$geometriesFileStem.shp")
    val geometriesCrs = LatLng

    val geometries = ShapeFileReader
      .readMultiPolygonFeatures(geometriesFile.getCanonicalPath)
      .map(_.geom)

    val from = LocalDate.of(2020, 7, 1).minusDays(90).atStartOfDay(ZoneOffset.UTC)
    val until = from plusDays 8

    assert(geometries.nonEmpty, s"no MultiPolygons found in $geometriesFile")

    val catalogApiSpy = spy(new DefaultCatalogApi(endpoint))

    val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", catalogApiSpy,
      new DefaultProcessApi(endpoint), authorizer, sampleType = FLOAT32, maxSpatialResolution = CellSize(0.0001,0.0001))

    // should fail while querying the Catalog API so no RDD evaluation is necessary/desired
    pyramidFactory.datacube_seq(
      polygons = geometries.toArray,
      polygons_crs = geometriesCrs,
      from_datetime = ISO_OFFSET_DATE_TIME format from,
      until_datetime = ISO_OFFSET_DATE_TIME format until,
      band_names = util.Arrays.asList("VH", "VV"),
      metadata_properties = util.Collections.emptyMap(),
      dataCubeParameters = new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )

    verify(catalogApiSpy, atLeastOnce()).search(eqTo("sentinel-1-grd"), any(), eqTo(LatLng),
      eqTo(from), eqTo(ZonedDateTime.parse("2020-04-09T23:59:59.999999999Z")), any(), eqTo(util.Collections.emptyMap()))
  }

  @Test
  def testPolygonOnEdgeOfSentinelFeature(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"

    val catalogApi = new DefaultCatalogApi(endpoint)

    val pyramidFactory = new PyramidFactory("sentinel-2-l2a", "sentinel-2-l2a", catalogApi,
      new DefaultProcessApi(endpoint), authorizer, sampleType = FLOAT32)

    val polygons_crs = CRS.fromEpsgCode(32630)
    val multiPolygon = {
      val in = Source.fromURL(getClass.getResource("/testPolygonOnEdgeOfSentinelFeature.geojson"))
      try MultiPolygon(GeoJson.parse[Polygon](in.mkString)).reproject(LatLng, polygons_crs)
      finally in.close()
    }

    val from = "2018-10-07T00:00:00+00:00"
    val until = "2018-10-08T00:00:00+00:00"

    val datacubeParams = new DataCubeParameters()
    datacubeParams.tileSize = 256
    datacubeParams.layoutScheme = "FloatingLayoutScheme"
    datacubeParams.partitionerIndexReduction = 7

    // Should not throw any of the 2 following errors after #128 fix:
    // - "Cannot create a polygon with exterior with fewer than 4 points: LINEARRING EMPTY"
    // - "NoSuchFeaturesException: no features found for criteria: ..."
    val ret = pyramidFactory.datacube_seq(
      polygons = Array(multiPolygon),
      polygons_crs = polygons_crs,
      from_datetime = from,
      until_datetime = until,
      band_names = util.Arrays.asList("B03"),
      metadata_properties = util.Collections.emptyMap(),
      datacubeParams,
      correlationId = testClassScopeMetadataTracker.scope,
    )
    println(ret)
  }

  @Test
  def testMetadata(): Unit = {
    val extent = Extent(-55.8071, -6.7014, -55.7933, -6.6703)
    BatchJobMetadataTracker.setGlobalTracking(true)
    val from = "2019-06-01T00:00:00Z"
    val to = "2019-06-11T00:00:00Z"
    val bandNames = Seq("VV", "VH", "HV", "HH").asJava

    val endpoint = "https://services.sentinel-hub.com"
    val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer)

    val multiPolygons = Array(MultiPolygon(extent.toPolygon()))
    val pyramid = pyramidFactory.datacube_seq(
      multiPolygons,
      CRS.fromEpsgCode(4326),
      from,
      to,
      bandNames,
      metadata_properties = util.Collections.emptyMap[String, util.Map[String, Any]],
      new DataCubeParameters,
      correlationId = testClassScopeMetadataTracker.scope,
    )
    println(pyramid.length)

    val inputs = BatchJobMetadataTracker.tracker("").asDict()
    val links = inputs.get("links").asInstanceOf[util.HashMap[String, util.List[ProductIdAndUrl]]]

    println(links)

    links.get("sentinel-1-grd").forEach(p => assertTrue(p.getSelfUrl, p.getSelfUrl.startsWith("http")))
  }

  @Test
  def testTimeDimensionFilter(): Unit = {
    val extent = Extent(-55.8071, -6.7014, -55.7933, -6.6703)
    BatchJobMetadataTracker.setGlobalTracking(true)
    val from = "2019-06-01T00:00:00Z"
    val to = "2019-06-11T00:00:00Z"
    val bandNames = Seq("VV", "VH", "HV", "HH").asJava

    val endpoint = "https://services.sentinel-hub.com"
    val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer)

    val dataCubeParameters = new DataCubeParameters()
    dataCubeParameters.setTimeDimensionFilter(new Object())

    val multiPolygons = Array(MultiPolygon(extent.toPolygon()))

    try {
      pyramidFactory.datacube_seq(
        multiPolygons,
        CRS.fromEpsgCode(4326),
        from,
        to,
        bandNames,
        metadata_properties = util.Collections.emptyMap[String, util.Map[String, Any]],
        dataCubeParameters,
        correlationId = testClassScopeMetadataTracker.scope,
      )
    } catch {
      case e: IllegalArgumentException =>
        assertTrue(e.getRootCause.getClass.toString, e.getRootCause.isInstanceOf[IllegalArgumentException])
    }
  }

  @Test
  def testErrorsFailResultRequest(): Unit = {
    // /result calls don't do tracking (OPENEO_BATCH_JOB_ID is not set) and fail on every error (error ratio 0.0)
    trackingOff()
    val noSoftErrorsRatio = 0.0

    try {
      val endpoint = "https://services.sentinel-hub.com"
      val processingOptions =
        Map("demInstance" -> "retteketet", "backCoeff" -> "GAMMA0_TERRAIN", "orthorectify" -> true).asJava

      val boundingBox = {
        val extent = Extent(-0.20057735970203852, 53.24811471153172, -0.19884885496214083, 53.24840102826777)
        val crs = UTM.getZoneCrs(extent.center.x, extent.center.y)
        ProjectedExtent(extent.reproject(LatLng, crs), crs)
      }

      val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
        new DefaultProcessApi(endpoint), authorizer, processingOptions, sampleType = FLOAT32,
        maxSoftErrorsRatio = noSoftErrorsRatio)

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        polygons = Array(MultiPolygon(boundingBox.extent.toPolygon())),
        polygons_crs = boundingBox.crs,
        from_datetime = "2018-10-07T00:00:00Z",
        until_datetime = "2018-10-08T00:00:00Z",
        band_names = util.Arrays.asList("VV", "VH"),
        metadata_properties = util.Collections.emptyMap(),
        dataCubeParameters = new DataCubeParameters,
        correlationId = testClassScopeMetadataTracker.scope,
      )

      layer.isEmpty() // force evaluation
      fail("should have thrown a SentinelHubException")
    } catch {
      case e: SparkException =>
        val SentinelHubException(_, 400, _, responseBody) = e.getRootCause
        assertTrue(responseBody, responseBody contains "Invalid DEM instance: retteketet")
    }

    val trackedMetadata = BatchJobMetadataTracker.tracker("").asDict()
    val numFailedRequests = trackedMetadata.get(SH_FAILED_TILE_REQUESTS).asInstanceOf[Long]

    assertEquals(s"expected exactly zero tracked failed tile requests but got $numFailedRequests instead",
      0, numFailedRequests)
  }

  @Ignore("not to be run automatically")
  @Test
  def testFixedAccessToken(): Unit = {
    val endpoint = "https://sh.dataspace.copernicus.eu"
    val date = ZonedDateTime.of(LocalDate.of(2018, 8, 6), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val collectionId = "sentinel-3-olci"
    val datasetId = collectionId
    // get it from https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token
    val accessToken = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJYVUh3VWZKaHVDVWo0X3k4ZF8xM0hxWXBYMFdwdDd2anhob2FPLUxzREZFIn0.eyJleHAiOjE2ODMxOTA4NjgsImlhdCI6MTY4MzE5MDI2OCwianRpIjoiNzdlY2VmZjUtZmM0My00NWNjLWIwNjYtNTUwN2YyNGQ0ZDQ1IiwiaXNzIjoiaHR0cHM6Ly9pZGVudGl0eS5kYXRhc3BhY2UuY29wZXJuaWN1cy5ldS9hdXRoL3JlYWxtcy9DRFNFIiwic3ViIjoiMjIyNjRjYWUtODA3Mi00MzUxLWIxYjYtZDMyYzE3M2VjZTc5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoic2gtMDVmOTM5YjUtODQ0NS00ZDVkLWIwNjAtMTk4OGI0N2Q2MzZhIiwic2NvcGUiOiJlbWFpbCBwcm9maWxlIHVzZXItY29udGV4dCIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiY2xpZW50SWQiOiJzaC0wNWY5MzliNS04NDQ1LTRkNWQtYjA2MC0xOTg4YjQ3ZDYzNmEiLCJjbGllbnRIb3N0IjoiMTkzLjE5MC4xODkuMTIiLCJvcmdhbml6YXRpb25zIjpbImRlZmF1bHQtOTc2MGU1OWItNTY2ZC00MmQxLWI2NWItMzRlZDE4NzlkYThhIl0sInVzZXJfY29udGV4dF9pZCI6IjYxNjEyZmM4LWFkNjQtNDU1ZC04YTFmLTViZmFlZTUwNGNhMCIsImNvbnRleHRfcm9sZXMiOnt9LCJjb250ZXh0X2dyb3VwcyI6WyIvb3JnYW5pemF0aW9ucy9kZWZhdWx0LTk3NjBlNTliLTU2NmQtNDJkMS1iNjViLTM0ZWQxODc5ZGE4YS8iXSwicHJlZmVycmVkX3VzZXJuYW1lIjoic2VydmljZS1hY2NvdW50LXNoLTA1ZjkzOWI1LTg0NDUtNGQ1ZC1iMDYwLTE5ODhiNDdkNjM2YSIsInVzZXJfY29udGV4dCI6ImRlZmF1bHQtOTc2MGU1OWItNTY2ZC00MmQxLWI2NWItMzRlZDE4NzlkYThhIiwiY2xpZW50QWRkcmVzcyI6IjE5My4xOTAuMTg5LjEyIn0.YCHjgV436G5BgCw-c_TmG55a8HlsWnHc79wCrVJ2M0HHSnSdJXzbH3nVy06wbc2UMnzhH_7DeD1_PjSDSuixVVTXI45DUy1gk4tP9etiuzBOLC7sy01skIuUoDvYzeCPDX9g5VEXqcjFUqJx0ydhCX-ewDSuH2cjS5wd7WqMsUqrWkfRNCcyu8qFdzbIKiCznjnYD3Et1Dxef-7m2ZmnaqUr8Xvw77GXuKmr9aoplbAhvfc6vLp4ffwWvaQPwek2v81PV4cIS-8Il4YXapdTEuoZIhbJNczKQPHMut6-3XOu8UAXY19GexuqqATmZIAgJZLqNAmZItUHR_xVcnU8Mw"

    val pyramidFactory = PyramidFactory.withFixedAccessToken(endpoint, collectionId, datasetId, accessToken,
      processingOptions = Collections.emptyMap(), sampleType = FLOAT32,
      maxSpatialResolution = CellSize(0.00297619047619, 0.00297619047619), maxSoftErrorsRatio = 0.0)

    testLayer(pyramidFactory, "sentinel-3-olci", date, Seq("B02", "B17", "B19"))
  }

  @Ignore("not to be run automatically")
  @Test
  def testCustomAuthApi(): Unit = {
    val endpoint = "https://sh.dataspace.copernicus.eu"
    val date = ZonedDateTime.of(LocalDate.of(2018, 8, 6), LocalTime.MIDNIGHT, ZoneOffset.UTC)
    val collectionId = "sentinel-3-olci"
    val datasetId = collectionId
    val authApiUrl = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    val clientId = "???"
    val clientSecret = "!!!"

    val pyramidFactory = PyramidFactory.withCustomAuthApi(endpoint, collectionId, datasetId,
      authApiUrl, clientId, clientSecret,
      processingOptions = Collections.emptyMap(), sampleType = FLOAT32,
      maxSpatialResolution = CellSize(0.00297619047619, 0.00297619047619), maxSoftErrorsRatio = 0.0)

    testLayer(pyramidFactory, "sentinel-3-olci", date, Seq("B02", "B17", "B19"))
  }
}
