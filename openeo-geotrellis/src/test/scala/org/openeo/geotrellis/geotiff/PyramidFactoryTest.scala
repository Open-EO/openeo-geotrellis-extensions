package org.openeo.geotrellis.geotiff

import java.net.URI
import java.time.LocalTime.MIDNIGHT
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.{ISO_LOCAL_DATE, ISO_OFFSET_DATE_TIME}
import java.time.{LocalDate, ZonedDateTime}

import geotrellis.layer._
import geotrellis.proj4.{CRS, LatLng}
import geotrellis.raster._
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.raster.io.geotiff.{GeoTiff, MultibandGeoTiff}
import geotrellis.spark._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.util.SparkUtils
import geotrellis.store.s3.util.S3RangeReader
import geotrellis.vector.{Extent, MultiPolygon, ProjectedExtent}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Ignore, Test}
import org.openeo.geotrellis.{OpenEOProcesses, ProjectedPolygons}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

object PyramidFactoryTest {
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
    val to = from

    singleBandGeoTiffFromDisk(
      globPattern = "/data/MTDA/CGS_S2/CGS_S2_FAPAR/2019/04/24/*/*/10M/*_FAPAR_10M_V102.tif", from, to)
  }

  @Test
  def singleBandGeoTiffFromDiskForMultipleDates(): Unit = {
    val from = ZonedDateTime.of(LocalDate.of(2019, 4, 24), MIDNIGHT, UTC)
    val to = from plusDays 2

    singleBandGeoTiffFromDisk(
      globPattern = "file:/data/MTDA/CGS_S2/CGS_S2_FAPAR/2019/04/2[34567]/*/*/10M/*_FAPAR_10M_V102.tif", from, to)
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

      MultibandGeoTiff(multibandTile, extent, layer.metadata.crs)
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
      date_regex = raw".*_(\d{4})(\d{2})(\d{2})T\d{6}\.tiff"
    )

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.pyramid_seq(boundingBox.extent, srs,
      ISO_OFFSET_DATE_TIME format from, ISO_OFFSET_DATE_TIME format to)

    val (maxZoom, _) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(14, maxZoom)

    saveLayerAsGeoTiff(pyramid, boundingBox, zoom = 10)
  }

  @Ignore("there's no dedicated S3 bucket yet")
  @Test
  def sentinelHubBatchProcessApiGeoTiffFromS3ForMultipleDates(): Unit = {
    assertNotNull("aws.accessKeyId is not set", System.getProperty("aws.accessKeyId"))
    assertNotNull("aws.secretAccessKey is not set", System.getProperty("aws.secretAccessKey"))
    System.setProperty("aws.region", "eu-central-1")

    val boundingBox = ProjectedExtent(Extent(2.59003, 51.069, 2.8949, 51.2206), CRS.fromEpsgCode(4326))

    val batchProcessId = "8e1f83d5-1a65-4de3-9430-ba0435533647"

    // the results for this batch process obviously only contain the dates that were requested in the first place so
    // no additional key filtering is necessary here
    val pyramidFactory = PyramidFactory.from_s3(
      s3_uri = s"s3://openeo-sentinelhub-vito-test/$batchProcessId/",
      key_regex = raw".*\.tif",
      date_regex = raw".*_(\d{4})(\d{2})(\d{2}).tif",
      recursive = true
    )

    val srs = s"EPSG:${boundingBox.crs.epsgCode.get}"
    val pyramid = pyramidFactory.datacube_seq(ProjectedPolygons(Array(boundingBox.extent.toPolygon()), srs),
      from_date = null, to_date = null)

    val (maxZoom, _) = pyramid.maxBy { case (zoom, _) => zoom }
    assertEquals(0, maxZoom)

    // FIXME: obvious artifacts in the geotiff, needs something like MosaicRasterSource
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

  private def saveAsGeotiff(layer: MultibandTileLayerRDD[SpaceTimeKey], at: ZonedDateTime, path: String) = {
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

  @Ignore("not a real test but trying to pass a custom S3 client to a GeoTiffRasterSource")
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
