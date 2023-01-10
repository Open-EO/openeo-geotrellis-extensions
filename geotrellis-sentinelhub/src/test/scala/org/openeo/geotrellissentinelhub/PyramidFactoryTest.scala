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
import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.hamcrest.{CustomMatcher, Matcher}
import org.junit.Assert.{assertEquals, assertThat, assertTrue, fail}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.Mockito._
import org.openeo.geotrelliscommon.BatchJobMetadataTracker.{SH_FAILED_TILE_REQUESTS, SH_PU}
import org.openeo.geotrelliscommon.{BatchJobMetadataTracker, DataCubeParameters, SparseSpaceTimePartitioner}
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

object PyramidFactoryTest {
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
    Map[String, Feature[Geometry, ZonedDateTime]] = {
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

  @BeforeClass def tracking(): Unit ={
    BatchJobMetadataTracker.setGlobalTracking(true)
  }

  @AfterClass def trackingOff(): Unit ={
    BatchJobMetadataTracker.setGlobalTracking(false)
  }

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
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]],
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
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]]) // https://github.com/scala/bug/issues/8911

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
    val expected = referenceRaster("utm.tif")

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
        new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10,10))

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(utmBoundingBox.extent.toPolygon())), utmBoundingBox.crs,
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B08", "B04", "B03").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]]
      )

      assertTrue(layer.partitioner.get.isInstanceOf[SpacePartitioner[SpaceTimeKey]])

      val spatialLayer = layer
        .toSpatial()
        .crop(utmBoundingBox.extent)
        .cache()

      val actual @ Raster(multibandTile, extent) = spatialLayer.stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/utm.tif")

      assertEquals(expected, actual)

      val trackedMetadata = BatchJobMetadataTracker.tracker("").asDict()
      val numFailedRequests = trackedMetadata.get(SH_FAILED_TILE_REQUESTS).asInstanceOf[Long]

      assertTrue(s"unexpected number of failed tile requests: $numFailedRequests", numFailedRequests >= 0)
    } finally sc.stop()
  }

  @Test
  def testUtmSparse(): Unit = {
    implicit val sc: SparkContext = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      // small (1 tile request) regions in the upper left and lower right corners of [2.59003, 51.069, 2.8949, 51.2206]
      val upperLeftBoundingBox =
        Extent(xmin = 2.590670585632324, ymin = 51.219034670299344, xmax = 2.5927734375, ymax = 51.22005609157961)
      val lowerRightBoundingBox =
        Extent(xmin = 2.888975143432617, ymin = 51.06977173805457, xmax = 2.8932666778564453, ymax = 51.07165938028684)

      val polygons = Array(upperLeftBoundingBox, lowerRightBoundingBox)
        .map(extent => MultiPolygon(extent.toPolygon()))

      val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

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
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B08", "B04", "B03").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]]
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
        .toSpatial()
        .cache()

      val utmPolygonsExtent = utmPolygons.toSeq.extent
      val Some(Raster(multibandTile, extent)) = spatialLayer
        .sparseStitch(utmPolygonsExtent)
        .map(_.crop(utmPolygonsExtent))

      val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/utm_sparse.tif")

      assertEquals(2, processApiSpy.getTileCount)
      // TODO: compare these regions against those for the non-sparse case (testUtm)
    } finally sc.stop()
  }

  @Test
  def testOverlappingPolygons(): Unit = {
    implicit val sc: SparkContext = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
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
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("VH", "VV").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]]
      )

      val spatialLayer = layer
        .toSpatial()
        .cache()

      val utmPolygonsExtent = utmPolygons.toSeq.extent
      val Some(Raster(multibandTile, extent)) = spatialLayer
        .sparseStitch(utmPolygonsExtent)
        .map(_.crop(utmPolygonsExtent))

      val tif = MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/overlapping.tif")
    } finally sc.stop()
  }

  @Test
  def testPolarizationDataFilter(): Unit = {
    val expected = referenceRaster("polarization.tif")

    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
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
    } finally sc.stop()
  }

  private def sparseSentinel1Layer(polygon: Array[MultiPolygon], crs:CRS, date: ZonedDateTime): MultibandTileLayerRDD[SpaceTimeKey] = {
    val endpoint = "https://services.sentinel-hub.com"
    val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, maxSpatialResolution = CellSize(10, 10),
      sampleType = FLOAT32)

    val Seq((_, layer)) = pyramidFactory.datacube_seq(
      polygon, crs,
      from_date = ISO_OFFSET_DATE_TIME format date,
      to_date = ISO_OFFSET_DATE_TIME format date,
      band_names = Seq("HV", "HH").asJava,
      metadata_properties = Collections.singletonMap("polarization", Collections.singletonMap("eq", "DH"))
    )
    layer
  }

  @Ignore("the actual collection ID is a secret")
  @Test
  def testPlanetScope(): Unit = {
    import scala.io.Source

    val planetCollectionId = {
      val in = Source.fromFile("/tmp/african_script_contest_collection_id")

      try in.mkString.trim
      finally in.close()
    }

    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
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
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B3", "B2", "B1").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]]
      )

      val spatialLayer = layer
        .toSpatial()
        .cache()

      val Raster(multibandTile, extent) = spatialLayer
        .stitch()
        .crop(utmBoundingBox.extent) // it's jumping around again

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/testPlanetScope.tif")
    } finally sc.stop()
  }

  @Ignore("the actual collection ID is a secret")
  @Test
  def testPlanetScopeCatalogReturnsMultiPolygonFeatures(): Unit = {
    import scala.io.Source

    val planetCollectionId = {
      val in = Source.fromFile("/tmp/african_script_contest_collection_id")

      try in.mkString.trim
      finally in.close()
    }

    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
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
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B2").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]]
      )

      val spatialLayer = layer
        .toSpatial()
        .cache()

      val Raster(multibandTile, extent) = spatialLayer
        .stitch()
        .crop(utmBoundingBox.extent) // it's jumping around again

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/testPlanetScopeCatalogReturnsMultiPolygonFeatures.tif")
    } finally sc.stop()
  }

  @Ignore("the actual collection ID is a secret")
  @Test
  def testPlanetScopeCatalogReturnsPolygonFeatures(): Unit = {
    import scala.io.Source

    val planetCollectionId = {
      val in = Source.fromFile("/tmp/uc8_collection_id")

      try in.mkString.trim
      finally in.close()
    }

    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
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
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B4").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]]
      )

      val spatialLayer = layer
        .toSpatial()
        .cache()

      val Raster(multibandTile, extent) = spatialLayer
        .stitch()
        .crop(utmBoundingBox.extent) // it's jumping around again

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/testPlanetScopeCatalogReturnsPolygonFeatures.tif")
    } finally sc.stop()
  }

  @Test
  def testEoCloudCover(): Unit = {
    val endpoint = "https://services.sentinel-hub.com"
    val date = ZonedDateTime.of(LocalDate.of(2019, 9, 21), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val pyramidFactory = new PyramidFactory("sentinel-2-l2a", "sentinel-2-l2a", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer)

    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      val boundingBox =
        ProjectedExtent(Extent(471275.3098352262, 5657503.248379398, 492660.20213888795, 5674436.759279663),
          CRS.fromEpsgCode(32631))

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format date,
        band_names = Seq("B04", "B03", "B02").asJava,
        metadata_properties = Collections.singletonMap("eo:cloud_cover", Collections.singletonMap("lte", 20))
      )

      val spatialLayer = layer
        .toSpatial()
        .cache()

      val Raster(multibandTile, extent) = spatialLayer
        .crop(boundingBox.extent)
        .stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/testEoCloudCover.tif")
    } finally sc.stop()
  }

  @Test
  def testMapzenDem(): Unit = {
    val endpoint = "https://services-uswest2.sentinel-hub.com"
    val date = ZonedDateTime.now(ZoneOffset.UTC)
    val to = date plusDays 100

    // from https://collections.eurodatacube.com/stac/mapzen-dem.json
    val maxSpatialResolution = CellSize(0.000277777777778, 0.000277777777778)

    val pyramidFactory = new PyramidFactory(collectionId = null, datasetId = "dem", new DefaultCatalogApi(endpoint),
      new DefaultProcessApi(endpoint), authorizer, sampleType = FLOAT32, maxSpatialResolution = maxSpatialResolution)

    val sc = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      val boundingBox = ProjectedExtent(Extent(2.59003, 51.069, 2.8949, 51.2206), LatLng)

      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
        from_date = ISO_OFFSET_DATE_TIME format date,
        to_date = ISO_OFFSET_DATE_TIME format to,
        band_names = Seq("DEM").asJava,
        metadata_properties = Collections.emptyMap[String, util.Map[String, Any]]
      )

      val spatialLayer = layer
        .toSpatial(to.toLocalDate.atStartOfDay(ZoneOffset.UTC))
        .cache()

      val Raster(multibandTile, extent) = spatialLayer
        .crop(boundingBox.extent)
        .stitch()

      val tif = MultibandGeoTiff(multibandTile, extent, spatialLayer.metadata.crs, geoTiffOptions)
      tif.write(s"/tmp/testMapzenDem_to.tif")
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
          ISO_OFFSET_DATE_TIME format to,
          band_names = Seq("VH", "VV").asJava,
          metadata_properties = Collections.singletonMap("orbitDirection", Collections.singletonMap("eq", "DESCENDING"))
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
    } finally sc.stop()
  }

  @Test
  def testSoftErrors(): Unit = {
    implicit val sc: SparkContext = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

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
        from_date = "2016-11-10T00:00:00Z",
        to_date = "2016-11-10T00:00:00Z",
        band_names = util.Arrays.asList("VH", "VV"),
        metadata_properties = util.Collections.emptyMap()
      )

      layer.isEmpty() // force evaluation
    }

    try {
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
    } finally sc.stop()
  }

  @Test
  def testSentinel5PL2DuplicateRequestsDatacube_seq(): Unit = {
    def layerFromDatacube_seq(pyramidFactory: PyramidFactory, boundingBox: ProjectedExtent, date: String,
                              bandNames: Seq[String], metadata_properties: util.Map[String, util.Map[String, Any]]):
    MultibandTileLayerRDD[SpaceTimeKey] = {
      val Seq((_, layer)) = pyramidFactory.datacube_seq(
        Array(MultiPolygon(boundingBox.extent.toPolygon())), boundingBox.crs,
        from_date = date,
        to_date = date,
        band_names = bandNames.asJava,
        metadata_properties = metadata_properties
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
    def layerFromPyramid_seq(pyramidFactory: PyramidFactory, boundingBox: ProjectedExtent, date: String,
                             bandNames: Seq[String], metadata_properties: util.Map[String, util.Map[String, Any]]):
    MultibandTileLayerRDD[SpaceTimeKey] = {
      val (_, baseLayer) = pyramidFactory.pyramid_seq(boundingBox.extent, s"EPSG:${boundingBox.crs.epsgCode.get}",
        from_date = date,
        to_date = date,
        band_names = bandNames.asJava,
        metadata_properties = metadata_properties
      ).maxBy { case (zoom, _) => zoom }

      baseLayer
    }

    // max-zoom ZoomedLayoutScheme(WebMercator) requires 4 tile requests to cover the input extent
    val expectedGetTileCount = 4

    testSentinel5PL2DuplicateRequests(layerFromPyramid_seq, expectedGetTileCount,
      "/tmp/testSentinel5PL2DuplicateRequestsPyramid_seq.tif")
  }

  private def testSentinel5PL2DuplicateRequests(layerFromSeq: (PyramidFactory, ProjectedExtent, String, Seq[String],
    util.Map[String, util.Map[String, Any]]) => MultibandTileLayerRDD[SpaceTimeKey], expectedGetTileCount: Int,
                                                outputTiff: String): Unit = {
    // mimics /home/bossie/Documents/VITO/applying mask increases PUs drastically #95/process_graph_without_mask_smaller.json
    implicit val sc: SparkContext = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
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

      val layer = layerFromSeq(pyramidFactory, boundingBox, ISO_OFFSET_DATE_TIME format date, Seq("NO2"),
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
    } finally sc.stop()
  }

  @Test
  def testLargeNumberOfInputPolygons(): Unit = {
    val tempDir = temporaryFolder.getRoot
    val geometriesFileStem = "Fields_to_extract_2021_30SVH_RAW_0"

    for (extension <- Seq("cpg", "dbf", "prj", "shp", "shx")) {
      val geometriesFilename = s"$geometriesFileStem.$extension"
      val geometriesUrl = new URL(s"https://artifactory.vgt.vito.be/testdata-public/parcels/$geometriesFilename")
      FileUtils.copyURLToFile(geometriesUrl, new File(tempDir, geometriesFilename))
    }

    val endpoint = "https://services.sentinel-hub.com"

    val geometriesFile = new File(tempDir, s"$geometriesFileStem.shp")
    val geometriesCrs = LatLng

    val geometries = ShapeFileReader
      .readMultiPolygonFeatures(geometriesFile.getCanonicalPath)
      .map(_.geom)

    val from = LocalDate.of(2020, 7, 1).minusDays(90).atStartOfDay(ZoneOffset.UTC)
    val to = from plusWeeks 1

    assert(geometries.nonEmpty, s"no MultiPolygons found in $geometriesFile")

    val catalogApiSpy = spy(new DefaultCatalogApi(endpoint))

    val pyramidFactory = new PyramidFactory("sentinel-1-grd", "sentinel-1-grd", catalogApiSpy,
      new DefaultProcessApi(endpoint), authorizer, sampleType = FLOAT32)

    val sc: SparkContext = SparkUtils.createLocalSparkContext("local[*]", appName = getClass.getSimpleName)

    try {
      // should fail while querying the Catalog API so no RDD evaluation is necessary/desired
      pyramidFactory.datacube_seq(
        polygons = geometries.toArray,
        polygons_crs = geometriesCrs,
        from_date = ISO_OFFSET_DATE_TIME format from,
        to_date = ISO_OFFSET_DATE_TIME format to,
        band_names = util.Arrays.asList("VH", "VV"),
        metadata_properties = util.Collections.emptyMap()
      )
    } finally sc.stop()

    verify(catalogApiSpy, atLeastOnce()).search(eqTo("sentinel-1-grd"), any(), eqTo(LatLng),
      eqTo(from), eqTo(ZonedDateTime.parse("2020-04-09T23:59:59.999999999Z")), any(), eqTo(util.Collections.emptyMap()))
  }
}
