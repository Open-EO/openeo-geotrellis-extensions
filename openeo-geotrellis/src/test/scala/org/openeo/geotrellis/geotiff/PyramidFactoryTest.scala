package org.openeo.geotrellis.geotiff

import java.net.URI
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.{ISO_LOCAL_DATE, ISO_OFFSET_DATE_TIME}
import java.time.{LocalDate, ZonedDateTime}
import java.util
import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.raster._
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiff, GeoTiffOptions, MultibandGeoTiff}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.SparkUtils
import geotrellis.store.s3.util.S3RangeReader
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Ignore, Test}
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

import java.util.zip.Deflater.BEST_COMPRESSION

object PyramidFactoryTest {
  private val sentinelHubBatchProcessResultsKeyRegex = raw".+\.tif".r
  private val sentinelHubBatchProcessResultsDateRegex = raw".+(\d{4})_?(\d{2})_?(\d{2}).*\.tif".r
  private val sentinelHubBucketName = "openeo-sentinelhub"

  private implicit var sc: SparkContext = _

  @BeforeClass
  def setupSpark(): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.rdd.compress","true")

    sc = SparkUtils.createLocalSparkContext(sparkMaster = "local[*]", appName = getClass.getSimpleName, sparkConf)
  }

  @AfterClass
  def tearDownSpark(): Unit = {
    sc.stop()
  }
}

class PyramidFactoryTest {
  import PyramidFactoryTest._

  @Test
  def singleBandGeoTiffFromDiskForSingleDate(): Unit = {
    val from = ZonedDateTime.of(LocalDate.of(2019, 4, 24), MIDNIGHT, UTC)
    val to = from.plusDays(1)

    singleBandGeoTiffFromDisk(
      globPattern = "/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2019/04/24/*/10M/*_FAPAR_10M_V200.tif", from, to)
  }

  @Test
  def singleBandGeoTiffFromDiskForMultipleDates(): Unit = {
    val from = ZonedDateTime.of(LocalDate.of(2019, 4, 24), MIDNIGHT, UTC)
    val to = from plusDays 2

    singleBandGeoTiffFromDisk(
      globPattern = "file:/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2019/04/2[34567]/*/10M/*_FAPAR_10M_V200.tif", from, to)
  }

  @Test
  def singleBandGeoTiffFromDiskForSingleFixedDate(): Unit = {
    val singlePath = "file:/data/MTDA/TERRASCOPE_Sentinel2/FAPAR_V2/2019/04/24/S2B_20190424T143759_19HCC_FAPAR_V200/20M/S2B_20190424T143759_19HCC_FAPAR_20M_V200.tif"
    val singleDate = LocalDate.of(2019, 4, 24).atStartOfDay(UTC)

    val pyramidFactory = PyramidFactory.from_disk(
      timestamped_paths = util.Collections.singletonMap(singlePath, singleDate format ISO_OFFSET_DATE_TIME)
    )

    val boundingBox = ProjectedExtent(Extent(-70.12, -34.38, -70.06, -34.34), LatLng)
    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, bbox_srs = s"EPSG:${boundingBox.crs.epsgCode.get}",
      from_date = singleDate.toString, to_date = singleDate.plusDays(1).toString)

    val (maxZoom, baseLayer) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(13, maxZoom)

    baseLayer.cache()

    assertFalse("base layer contains no tiles!", baseLayer.isEmpty())

    val uniqueTimestamps = baseLayer.keys
      .map(_.time)
      .distinct().collect()

    assertArrayEquals(Array[Object](singleDate), uniqueTimestamps.asInstanceOf[Array[Object]])

    saveLayerAsGeoTiff(pyramid, boundingBox, maxZoom)
  }

  @Test
  def loadResultGeoTiffFromDiskForSingleFixedDate(): Unit = {
    val singlePath = "file:/data/projects/OpenEO/automated_test_files/load_result_openEO_2019-09-22Z.tif"
    val singleDate = LocalDate.of(2019, 9, 22).atStartOfDay(UTC)

    val pyramidFactory = PyramidFactory.from_disk(
      timestamped_paths = util.Collections.singletonMap(singlePath, singleDate format ISO_OFFSET_DATE_TIME)
    )

    val smallerBoundingBox = ProjectedExtent(Extent(2.6951, 51.1160, 2.7822, 51.1672), LatLng)

    val dataCubeParameters = new org.openeo.geotrelliscommon.DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"

    val pyramid = pyramidFactory.datacube_seq(
      ProjectedPolygons.reproject(
        ProjectedPolygons.fromExtent(smallerBoundingBox.extent, s"EPSG:${smallerBoundingBox.crs.epsgCode.get}"), 32631),
      from_date = "1970-01-01T00:00:00+00:00",
      to_date = "2070-01-01T00:00:00+00:00",
      metadata_properties = null,
      correlationId = null,
      dataCubeParameters
    )

    val Seq((maxZoom, baseLayer)) = pyramid
    assertEquals(0, maxZoom)

    baseLayer.cache()

    assertFalse("base layer contains no tiles!", baseLayer.isEmpty())

    val uniqueTimestamps = baseLayer.keys
      .map(_.time)
      .distinct().collect()

    assertArrayEquals(Array[Object](singleDate), uniqueTimestamps.asInstanceOf[Array[Object]])

    saveLayerAsGeoTiff(pyramid, smallerBoundingBox, maxZoom)
  }

  private def singleBandGeoTiffFromDisk(globPattern: String, from: ZonedDateTime, to: ZonedDateTime): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val pyramidFactory = PyramidFactory.from_disk(
      globPattern,
      date_regex = raw".*\/S2._(\d{4})(\d{2})(\d{2})T.*"
    )

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
      ISO_OFFSET_DATE_TIME format from, ISO_OFFSET_DATE_TIME format to)

    val (maxZoom, _) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(14, maxZoom)

    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = 10)
  }

  private def saveLayerAsGeoTiff(pyramid: Seq[(Int, MultibandTileLayerRDD[SpaceTimeKey])], boundingBox: ProjectedExtent,
                                 zoom: Int): Unit = {
    val layer = pyramid
      .find { case (index, _) => index == zoom }
      .map { case (_, layer) => layer }
      .get.cache()

    println(s"got ${layer.count()} tiles")

    val timestamps = layer.keys
      .map(_.time)
      .distinct()
      .collect()
      .sortWith(_ isBefore _)

    for (timestamp <- timestamps) {
      val Raster(multibandTile, extent) = layer
        .toSpatial(timestamp)
        .stitch()
        .crop(boundingBox.reproject(layer.metadata.crs))

      val options = GeoTiffOptions(DeflateCompression(BEST_COMPRESSION))

      MultibandGeoTiff(multibandTile, extent, layer.metadata.crs, options)
        .write(s"/tmp/stitched_${ISO_LOCAL_DATE format timestamp}_$zoom.tif")
    }
  }

  @Ignore("currently works only against amazonaws.com")
  @Test
  def singleBandGeoTiffFromS3ForSingleDate(): Unit = {
    // otherwise the S3 client will keep retrying to access
    // http://169.254.169.254/latest/meta-data/iam/security-credentials/
    assertNotNull("aws.accessKeyId is not set", System.getProperty("aws.accessKeyId"))
    assertNotNull("aws.secretKey is not set", System.getProperty("aws.secretKey"))
    System.setProperty("aws.region", "eu-west-3")

    val boundingBox = ProjectedExtent(Extent(xmin = 679605.00, ymin = 5667337.31, xmax = 691784.50, ymax = 5678547.98),
      CRS.fromEpsgCode(32631))

    val from = ZonedDateTime.of(LocalDate.of(2018, 4, 28), MIDNIGHT, UTC)
    val to = from

    val pyramidFactory = PyramidFactory.from_s3(
      s3_uri = "s3://openeo-vito-test/cogs/",
      key_regex = raw".*_20180428T.*\.tiff",
      date_regex = raw".*_(\d{4})(\d{2})(\d{2})T\d{6}\.tiff",
      lat_lon = false
    )

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
      ISO_OFFSET_DATE_TIME format from, ISO_OFFSET_DATE_TIME format to)

    val (maxZoom, _) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(14, maxZoom)

    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = 10)
  }

  @Ignore("the bucket is being emptied because S3 costs are through the roof")
  @Test
  def sentinelHubBatchProcessApiGeoTiffFromS3ForMultipleDates(): Unit = {
    assertNotNull("AWS_ACCESS_KEY_ID is not set", System.getenv("AWS_ACCESS_KEY_ID"))
    assertNotNull("AWS_SECRET_ACCESS_KEY is not set", System.getenv("AWS_SECRET_ACCESS_KEY"))
    System.setProperty("aws.region", "eu-central-1")

    val boundingBox = ProjectedExtent(Extent(2.59003, 51.069, 2.8949, 51.2206), LatLng)
    val crs = CRS.fromEpsgCode(32631)
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(crs), crs)

    val batchProcessId = "7f3d98f2-4a9a-4fbe-adac-973f1cff5699"

    // the results for this batch process obviously only contain the dates that were requested in the first place so
    // no additional key filtering is necessary here
    val pyramidFactory = PyramidFactory.from_s3(
      s3_uri = s"s3://$sentinelHubBucketName/$batchProcessId/",
      key_regex = sentinelHubBatchProcessResultsKeyRegex.regex,
      date_regex = sentinelHubBatchProcessResultsDateRegex.regex,
      recursive = true,
      interpret_as_cell_type = "float32ud0"
    )

    val srs = s"EPSG:${reprojectedBoundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.datacube_seq(ProjectedPolygons(Array(reprojectedBoundingBox.extent.toPolygon()), srs),
      from_date = null, to_date = null)

    val (maxZoom, baseLayer) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(0, maxZoom)
    assertEquals(crs, baseLayer.metadata.crs)

    saveLayerAsGeoTiff(pyramid, reprojectedBoundingBox, zoom = maxZoom)
  }

  @Ignore
  @Test
  def assembledSentinelHubBatchProcessResultsFromS3(): Unit = {
    assertNotNull("AWS_ACCESS_KEY_ID is not set", System.getenv("AWS_ACCESS_KEY_ID"))
    assertNotNull("AWS_SECRET_ACCESS_KEY is not set", System.getenv("AWS_SECRET_ACCESS_KEY"))
    System.setProperty("aws.region", "eu-central-1")

    val boundingBox = ProjectedExtent(Extent(2.59003, 51.069, 2.8949, 51.2206), LatLng)
    val assembledFolder = "assembled_1261505205781045458"

    val pyramidFactory = PyramidFactory.from_s3(
      s3_uri = s"s3://$sentinelHubBucketName/$assembledFolder",
      key_regex = sentinelHubBatchProcessResultsKeyRegex.regex,
      date_regex = sentinelHubBatchProcessResultsDateRegex.regex,
      recursive = true,
      interpret_as_cell_type = "float32ud0",
      lat_lon = false
    )

    val pyramid = pyramidFactory.pyramid_seq(
      boundingBox.extent,
      bbox_srs = s"EPSG:${boundingBox.crs.epsgCode.get}",
      from_date = null,
      to_date = null
    )

    val (maxZoom, _) = pyramid.maxBy { case (zoom, _) => zoom }
    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = maxZoom)
  }

  @Ignore("the bucket is being emptied because S3 costs are through the roof")
  @Test
  def sentinelHubCard4LBatchProcessApiGeoTiffFromS3ForMultipleDates(): Unit = {
    assertNotNull("AWS_ACCESS_KEY_ID is not set", System.getenv("AWS_ACCESS_KEY_ID"))
    assertNotNull("AWS_SECRET_ACCESS_KEY is not set", System.getenv("AWS_SECRET_ACCESS_KEY"))
    System.setProperty("aws.region", "eu-central-1")

    val boundingBox = ProjectedExtent(Extent(35.666439, -6.23476, 35.861576, -6.075694), LatLng)

    val requestGroupId = "a894cae5-7193-48ed-80ad-901769483a46"

    val pyramidFactory = PyramidFactory.from_s3(
      s3_uri = s"s3://$sentinelHubBucketName/$requestGroupId/",
      key_regex = sentinelHubBatchProcessResultsKeyRegex.regex,
      date_regex = sentinelHubBatchProcessResultsDateRegex.regex,
      recursive = true,
      interpret_as_cell_type = "float32", // TODO: is float32ud0 in the Python code
      lat_lon = true
    )

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.datacube_seq(ProjectedPolygons.fromExtent(boundingBox.extent, srs),
      from_date = null, to_date = null)

    val (maxZoom, baseLayer) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(0, maxZoom)
    assertEquals(LatLng, baseLayer.metadata.crs)

    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = maxZoom)
  }

  @Ignore("added for debugging purposes")
  @Test
  def adjacentSentinelHubCard4LBatchProcessApiGeotiffs(): Unit = {
    val pyramidFactory = PyramidFactory.from_disk(
      glob_pattern = "/tmp/prod_ard/s1_rtc_*_2021_03_09_MULTIBAND.tif",
      date_regex = sentinelHubBatchProcessResultsDateRegex.regex,
      interpret_as_cell_type = "float32",
      lat_lon = true
    )

    val boundingBox = ProjectedExtent(Extent(12.03762, 41.908324, 12.511386, 42.133792), LatLng)

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.datacube_seq(ProjectedPolygons.fromExtent(boundingBox.extent, srs),
      from_date = null, to_date = null)

    val (maxZoom, _) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(0, maxZoom)

    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = maxZoom)
  }

  @Ignore("the bucket is being emptied because S3 costs are through the roof")
  @Test
  def sentinelHubBatchProcessApiGeoTiffFromS3ForMultipleDates_pyramid_seq(): Unit = {
    assertNotNull("AWS_ACCESS_KEY_ID is not set", System.getenv("AWS_ACCESS_KEY_ID"))
    assertNotNull("AWS_SECRET_ACCESS_KEY is not set", System.getenv("AWS_SECRET_ACCESS_KEY"))
    System.setProperty("aws.region", "eu-central-1")

    val boundingBox = ProjectedExtent(Extent(2.59003, 51.069, 2.8949, 51.2206), LatLng)

    val batchProcessId = "7f3d98f2-4a9a-4fbe-adac-973f1cff5699"

    // the results for this batch process obviously only contain the dates that were requested in the first place so
    // no additional key filtering is necessary here
    val pyramidFactory = PyramidFactory.from_s3(
      s3_uri = s"s3://$sentinelHubBucketName/$batchProcessId/",
      key_regex = sentinelHubBatchProcessResultsKeyRegex.regex,
      date_regex = sentinelHubBatchProcessResultsDateRegex.regex,
      recursive = true,
      interpret_as_cell_type = "float32ud0"
    )

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
      from_date = null, to_date = null)

    val (maxZoom, baseLayer) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(14, maxZoom)
    assertEquals(WebMercator, baseLayer.metadata.crs)

    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = maxZoom)
  }

  @Ignore("depends on data in AWS S3")
  @Test
  def sentinelHubBatchProcessApiGeoTiffFromS3BucketInDifferentRegion(): Unit = {
    assertNotNull("AWS_ACCESS_KEY_ID is not set", System.getenv("AWS_ACCESS_KEY_ID"))
    assertNotNull("AWS_SECRET_ACCESS_KEY is not set", System.getenv("AWS_SECRET_ACCESS_KEY"))
    System.clearProperty("aws.region")

    val boundingBox = ProjectedExtent(Extent(15.06, 48.06, 16.06, 49.06), LatLng)

    val differentRegionBucketName = "openeo-sentinelhub-uswest2"
    val batchProcessId = "5e2c5280-b900-4350-9e3a-fb25048bc207"

    val pyramidFactory = PyramidFactory.from_s3(
      s3_uri = s"s3://$differentRegionBucketName/$batchProcessId",
      //key_regex = raw".+_20191010\.tif".r.regex,  // single day
      key_regex = raw".+/33UWQ_1_9/_20191010\.tif".r.regex, // single tile
      date_regex = sentinelHubBatchProcessResultsDateRegex.regex,
      recursive = true,
      interpret_as_cell_type = "float32ud0",
      lat_lon = false
    )

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
    val projectedPolygons = ProjectedPolygons.reproject(ProjectedPolygons.fromExtent(boundingBox.extent, srs), 32633)
    val pyramid = pyramidFactory.datacube_seq(projectedPolygons, from_date = null, to_date = null)

    val (maxZoom, _) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(0, maxZoom)

    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = maxZoom)
  }

  @Test
  def joinLayers(): Unit = {
    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val from = ZonedDateTime.of(LocalDate.of(2019, 4, 24), MIDNIGHT, UTC)
    val to = from

    val zoom = 10

    val data = PyramidFactory.from_disk(
      glob_pattern = "/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/04/24/*/*/*_TOC-B02_10M_V102.tif",
      date_regex = raw".*\/S2._(\d{4})(\d{2})(\d{2})T.*"
    ).layer(boundingBox, from, to, zoom).cache()

    assertTrue(data.partitioner.contains(SpacePartitioner(data.metadata.bounds)))

    saveAsGeotiff(data, from, "/tmp/data.tif")

    val mask = PyramidFactory.from_disk(
      glob_pattern = "/data/MTDA/CGS_S2/CGS_S2_RADIOMETRY/2019/04/24/*/*/*_SHADOWMASK_10M_V102.tif",
      date_regex = raw".*\/S2._(\d{4})(\d{2})(\d{2})T.*"
    ).layer(boundingBox, from, to, zoom).cache()

    assertTrue(mask.partitioner.contains(SpacePartitioner(mask.metadata.bounds)))

    saveAsGeotiff(mask, from, "/tmp/mask.tif")

    val joinedLayer = new OpenEOProcesses().rasterMask(data, mask, replacement = null)
    assertTrue(joinedLayer.partitioner.contains(SpacePartitioner(joinedLayer.metadata.bounds)))

    saveAsGeotiff(joinedLayer, from, "/tmp/masked.tif")
  }

  private def saveAsGeotiff(layer: MultibandTileLayerRDD[SpaceTimeKey], at: ZonedDateTime, path: String): Unit = {
    val Raster(tile, extent) = layer.toSpatial(at).stitch()
    GeoTiff(tile, extent, layer.metadata.crs).write(path)
  }

  @Test
  def fromDiskIsLazy(): Unit = {
    val invalidPathPyramidFactory = PyramidFactory.from_disk(
      glob_pattern = "/does/not/exist/*.tif",
      date_regex = raw"S2._(\d{4})(\d{2})(\d{2}).*_FAPAR_10M_V.*tif"
    ) // succeeds

    val boundingBox = ProjectedExtent(Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206), LatLng)

    val from = ZonedDateTime.of(LocalDate.of(2019, 4, 24), MIDNIGHT, UTC)
    val to = from

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"

    try {
      invalidPathPyramidFactory.pyramid_seq(boundingBox.extent, srs,
        ISO_OFFSET_DATE_TIME format from, ISO_OFFSET_DATE_TIME format to)
      fail()
    } catch {
      case e: IllegalStateException if e.getMessage == "no raster sources found" => // expected failure
    }
  }

  @Ignore
  @Test
  def assembledSentinelHubBatchProcessResultsFromDisk(): Unit = {
    val boundingBox = ProjectedExtent(Extent(2.59003, 51.069, 2.8949, 51.2206), LatLng)
    val crs = CRS.fromEpsgCode(32631)
    val reprojectedBoundingBox = ProjectedExtent(boundingBox.reproject(crs), crs)

    val pyramidFactory = PyramidFactory.from_disk(
      glob_pattern = "file:///tmp/assembled_1168491449486718719//*.tif",
      date_regex = /* raw".+/(\d{4})(\d{2})(\d{2})-[A-Z0-9_]+\.tif".r.regex */ raw".+(\d{4})_?(\d{2})_?(\d{2}).*\.tif".r.regex,
      interpret_as_cell_type = "float32ud0",
      lat_lon = false
    )

    val srs = s"EPSG:${reprojectedBoundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.datacube_seq(ProjectedPolygons(Array(reprojectedBoundingBox.extent.toPolygon()), srs),
      from_date = null, to_date = null)

    val (maxZoom, baseLayer) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(0, maxZoom)
    assertEquals(crs, baseLayer.metadata.crs)

    saveLayerAsGeoTiff(pyramid, reprojectedBoundingBox, zoom = maxZoom)
  }

  @Ignore("requires a file with Terrascope credentials")
  @Test
  def authorizedGeoTiffFromUri(): Unit = {
    val singleDate = LocalDate.of(2020, 1, 5).atStartOfDay(UTC) format ISO_OFFSET_DATE_TIME

    val timestampedUris = util.Collections.singletonMap(
      "https://services.terrascope.be/download/Sentinel2/LAI_V2/2020/01/05/S2A_20200105T071301_39RVH_LAI_V200/20M/S2A_20200105T071301_39RVH_LAI_20M_V200.tif", singleDate)

    val boundingBox = ProjectedExtent(
      Extent(444068.6374407583498396, 2741875.7612559241242707, 449191.1196682464797050, 2747715.3909952607937157),
      CRS.fromEpsgCode(32639))

    val pyramidFactory = PyramidFactory.from_uris(timestampedUris)

    val dataCubeParameters = new org.openeo.geotrelliscommon.DataCubeParameters
    dataCubeParameters.layoutScheme = "FloatingLayoutScheme"

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.datacube_seq(ProjectedPolygons.fromExtent(boundingBox.extent, srs),
      from_date = singleDate, to_date = singleDate, metadata_properties = null, correlationId = null, dataCubeParameters)

    val Seq((maxZoom, _)) = pyramid
    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = maxZoom)
  }

  @Ignore("doesn't work, the S3Client instance is not propagated as RasterSources are manipulated")
  @Test
  def anonymousInnerClass(): Unit = {

    import geotrellis.raster.io.geotiff.reader.GeoTiffReader
    import geotrellis.util.StreamingByteReader


    def getByteReader(uri: String): StreamingByteReader = {
      val endpoint = "https://oss.eu-west-0.prod-cloud-ocb.orange-business.com:443"
      val region = "eu-west-0"
      val accessKey = System.getProperty("aws.accessKeyId")
      val secretKey = System.getProperty("aws.secretKey")

      val s3Client = S3Client.builder()
        .endpointOverride(new URI(endpoint))
        .region(Region.of(region))
        .credentialsProvider(StaticCredentialsProvider.create( AwsBasicCredentials.create(accessKey, secretKey)))
        .build()

      val rr = S3RangeReader(uri, s3Client)
      new StreamingByteReader(rr)
    }

    val uri = "s3://s2-ndvi/cogs/S2A_MSIL1C_20180401T105031_N0206_R051_T31UES_20180401T144530.tiff"
    val rasterSource = new GeoTiffRasterSource(uri) {
      @transient override lazy val tiff: MultibandGeoTiff =
        GeoTiffReader.readMultiband(getByteReader(uri), streaming = true)
    }

    val Some(Raster(multibandTile, extent)) = rasterSource.read(Extent(xmin = 516807.20, ymin = 5680949.70, xmax = 532827.11, ymax = 5695567.02))
    MultibandGeoTiff(multibandTile, extent, rasterSource.crs)
      .write(s"/tmp/s3.tif")
  }
}
