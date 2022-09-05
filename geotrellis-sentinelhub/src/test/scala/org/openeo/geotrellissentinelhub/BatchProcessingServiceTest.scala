package org.openeo.geotrellissentinelhub

import geotrellis.proj4.LatLng
import geotrellis.vector.io.json.GeoJson
import geotrellis.vector._
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.{Ignore, Rule, Test}

import java.util.{Arrays, Collections, Map => JMap, UUID}
import java.time.LocalTime
import scala.annotation.meta.getter
import scala.collection.JavaConverters._

class BatchProcessingServiceTest {
  private val endpoint = "https://services.sentinel-hub.com" // TODO: this depends on the dataset
  private val authorizer = new MemoizedAuthApiAccessTokenAuthorizer(Utils.clientId, Utils.clientSecret)
  private val batchProcessingService = new BatchProcessingService(endpoint, bucketName = "openeo-sentinelhub",
    authorizer)

  @(Rule @getter)
  val temporaryFolder = new TemporaryFolder
  private def collectingFolder = temporaryFolder.getRoot.toPath

  @Ignore
  @Test
  def startBatchProcess(): Unit = {
    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      bbox = Extent(2.59003, 51.069, 2.8949, 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2019-10-10T00:00:00+00:00",
      to_date = "2019-10-10T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startBatchProcessForOverlappingPolygons(): Unit = {
    val upperLeftPolygon =
      Extent(4.093673229217529, 50.39570215730746, 4.095818996429443, 50.39704266811707).toPolygon()
    val lowerRightPolygon =
      Extent(4.094831943511963, 50.39508660393027, 4.0970635414123535, 50.396317702692095).toPolygon()

    assertTrue("polygons do not overlap", upperLeftPolygon intersects lowerRightPolygon)

    val polygons = Array(
      MultiPolygon(upperLeftPolygon),
      MultiPolygon(lowerRightPolygon)
    )

    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      polygons,
      crs = LatLng,
      from_date = "2021-04-03T00:00:00+00:00",
      to_date = "2021-04-03T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startBatchProcessToCustomSubfolder(): Unit = {
    val subfolder = UUID.randomUUID().toString

    println(subfolder)

    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      bbox = Extent(2.59003, 51.069, 2.8949, 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2019-10-10T00:00:00+00:00",
      to_date = "2019-10-10T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any],
      subfolder
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startBatchProcessForOrbitDirection(): Unit = {
    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      bbox = Extent(2.59003, 51.069, 2.8949, 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2019-10-08T00:00:00+00:00",
      to_date = "2019-10-12T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      metadata_properties = Collections.singletonMap("orbitDirection", Collections.singletonMap("eq", "ASCENDING")),
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startBatchProcessForEoCloudCover(): Unit = {
    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-2-l2a",
      dataset_id = "sentinel-2-l2a",
      bbox = Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2019-09-21T00:00:00+00:00",
      to_date = "2019-09-21T00:00:00+00:00",
      band_names = Arrays.asList("B04", "B03", "B02"),
      SampleType.UINT16,
      metadata_properties = Collections.singletonMap("eo:cloud_cover", Collections.singletonMap("lte", 20)),
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startBatchProcessForSentinel2(): Unit = {
    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-2-l2a",
      dataset_id = "sentinel-2-l2a",
      bbox = Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2019-09-21T00:00:00+00:00",
      to_date = "2019-09-21T00:00:00+00:00",
      band_names = Arrays.asList("B04", "B03", "B02"),
      SampleType.UINT16,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startBatchProcessForModis(): Unit = {
    val batchProcessingService = new BatchProcessingService(endpoint = "https://services-uswest2.sentinel-hub.com",
      bucketName = "openeo-sentinelhub-uswest2", authorizer)

    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "modis",
      dataset_id = "MODIS",
      bbox = Extent(15.449523925781252, 48.57660713188407, 15.622558593749998, 48.6927734325279),
      bbox_srs = "EPSG:4326",
      from_date = "2019-10-01T00:00:00+00:00",
      to_date = "2019-10-01T00:00:00+00:00",
      band_names = Arrays.asList("B01", "B02"),
      SampleType.UINT16,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId), batchProcessingService))
  }

  @Ignore
  @Test
  def startBatchProcessForMapzenDem(): Unit = {
    val batchProcessingService = new BatchProcessingService(endpoint = "https://services-uswest2.sentinel-hub.com",
      bucketName = "openeo-sentinelhub-uswest2", authorizer)

    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = null,
      dataset_id = "dem",
      bbox = Extent(2.59003, 51.069, 2.8949, 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2020-01-01T00:00:00+00:00",
      to_date = "2020-01-01T00:00:00+00:00",
      band_names = Arrays.asList("DEM"),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId), batchProcessingService))
  }

  @Ignore
  @Test
  def startCachedBatchProcessForSentinel2(): Unit = {
    val subfolder = UUID.randomUUID().toString

    println(s"subfolder: $subfolder")

    // runs SHub batch process that puts its results into s3:///subfolder
    // collectingFolder is a directory on disk that contains symlinks to cached tiles
    val batchRequestId = batchProcessingService.start_batch_process_cached(
      collection_id = "sentinel-2-l2a",
      dataset_id = "sentinel-2-l2a",
      bbox = Extent(xmin = 2.59003, ymin = 51.069, xmax = 2.8949, ymax = 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2019-09-21T00:00:00+00:00",
      to_date = "2019-09-21T00:00:00+00:00",
      band_names = Arrays.asList("B04", "B03", "B02"),
      SampleType.UINT16,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any],
      subfolder,
      collectingFolder.toAbsolutePath.toString
    )

    if (batchRequestId != null) println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startPolygonalCachedBatchProcessForSentinel2(): Unit = {
    val subfolder = UUID.randomUUID().toString

    println(s"subfolder: $subfolder")

    val polygon = GeoJson.parse[Polygon](
      """
        |{
        |  "type":"Polygon",
        |  "coordinates":[
        |    [
        |      [
        |        2.80426025390625,
        |        51.03405383220282
        |      ],
        |      [
        |        2.89215087890625,
        |        51.062544053267686
        |      ],
        |      [
        |        2.8900909423828125,
        |        51.22580788296972
        |      ],
        |      [
        |        2.65594482421875,
        |        51.2425753584134
        |      ],
        |      [
        |        2.6374053955078125,
        |        51.06513320441178
        |      ],
        |      [
        |        2.80426025390625,
        |        51.03405383220282
        |      ]
        |    ]
        |  ]
        |}""".stripMargin)

    val polygons: Array[MultiPolygon] = Array(MultiPolygon(polygon))
    val polygonsCrs = LatLng

    val batchRequestId = batchProcessingService.start_batch_process_cached(
      collection_id = "sentinel-2-l2a",
      dataset_id = "sentinel-2-l2a",
      polygons,
      polygonsCrs,
      from_date = "2019-09-21T00:00:00+00:00",
      to_date = "2019-09-21T00:00:00+00:00",
      band_names = Arrays.asList("B04", "B03", "B02"),
      SampleType.UINT16,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any],
      subfolder,
      collectingFolder.toAbsolutePath.toString
    )

    if (batchRequestId != null) println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startLargePolygonCachedBatchProcessForSentinel1(): Unit = {
    val subfolder = UUID.randomUUID().toString

    println(s"subfolder: $subfolder")

    val malawiPolygon = GeoJson.parse[Polygon](
      """
        |{
        |  "type":"Polygon",
        |  "coordinates":[
        |    [
        |      [
        |        33.15374754008658,
        |        -13.579730951495556
        |      ],
        |      [
        |        33.15374754008658,
        |        -12.523064670912134
        |      ],
        |      [
        |        33.961499361964435,
        |        -12.523064670912134
        |      ],
        |      [
        |        33.961499361964435,
        |        -13.579730951495556
        |      ],
        |      [
        |        33.15374754008658,
        |        -13.579730951495556
        |      ]
        |    ]
        |  ]
        |}""".stripMargin)

    val polygons: Array[MultiPolygon] = Array(MultiPolygon(malawiPolygon))
    val polygonsCrs = LatLng

    val batchRequestId = batchProcessingService.start_batch_process_cached(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      polygons,
      polygonsCrs,
      from_date = "2020-11-25T00:00:00+00:00",
      to_date = "2020-11-25T00:00:00+00:00",
      band_names = Arrays.asList("VH"/*, "VV"*/),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Map(
        "backCoeff" -> "GAMMA0_TERRAIN",
        "orthorectify" -> true
      ).asJava,
      subfolder,
      collectingFolder.toAbsolutePath.toString
    )

    if (batchRequestId != null) println(awaitDone(Seq(batchRequestId)))
  }

  @Test
  def getBatchProcessStatus(): Unit = {
    val status =
      batchProcessingService.get_batch_process_status(batch_request_id = "7f3d98f2-4a9a-4fbe-adac-973f1cff5699")

    assertEquals("DONE", status)
  }

  @Test
  def getBatchProcess(): Unit = {
    val batch_process =
      batchProcessingService.get_batch_process(batch_request_id = "dd43f448-d582-40c5-9288-ae7a9c07ecbe")

    assertEquals("DONE", batch_process.status)
    assertEquals(91.55273710348410, batch_process.processing_units_spent.doubleValue(), 0.0001)
  }

  @Ignore
  @Test
  def startCard4LBatchProcesses(): Unit = {
    val requestGroupUuid = UUID.randomUUID().toString

    val batchRequestIds = batchProcessingService.start_card4l_batch_processes(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      bbox = Extent(35.666439, -6.23476, 35.861576, -6.075694),
      bbox_srs = "EPSG:4326",
      from_date = "2021-02-01T00:00:00+00:00",
      to_date = "2021-02-17T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV", "dataMask", "localIncidenceAngle"),
      dem_instance = null,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      subfolder = requestGroupUuid,
      requestGroupUuid
    )

    println(s"batch process(es) $batchRequestIds will write to ${batchProcessingService.bucketName}/$requestGroupUuid")

    println(awaitDone(batchRequestIds.asScala))

    new S3Service().download_stac_data(
      batchProcessingService.bucketName,
      requestGroupUuid,
      target_dir = "/tmp/saved_stac"
    )
  }

  @Ignore
  @Test
  def startCard4LBatchProcessesForOrbitDirection(): Unit = {
    val requestGroupUuid = UUID.randomUUID().toString

    val batchRequestIds = batchProcessingService.start_card4l_batch_processes(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      bbox = Extent(35.666439, -6.23476, 35.861576, -6.075694),
      bbox_srs = "EPSG:4326",
      from_date = "2021-01-25T00:00:00+00:00",
      to_date = "2021-02-17T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV", "dataMask", "localIncidenceAngle"),
      dem_instance = null,
      metadata_properties = Collections.singletonMap("orbitDirection", Collections.singletonMap("eq", "DESCENDING")),
      subfolder = requestGroupUuid,
      requestGroupUuid
    )

    println(s"batch process(es) $batchRequestIds will write to ${batchProcessingService.bucketName}/$requestGroupUuid")

    println(awaitDone(batchRequestIds.asScala))

    new S3Service().download_stac_data(
      batchProcessingService.bucketName,
      requestGroupUuid,
      target_dir = "/tmp/saved_stac"
    )
  }

  @Ignore
  @Test
  def startBatchProcessForSparsePolygons(): Unit = {
    val bboxLeft = Extent(3.7614440917968746, 50.737052666897405, 3.7634181976318355, 50.738139065342224)
    val bboxRight = Extent(4.3924713134765625, 50.741235162650355, 4.3979644775390625, 50.74297323282792)

    val polygons = Array(bboxLeft, bboxRight)
      .map(bbox => MultiPolygon(Seq(bbox.toPolygon())))

    val crs = LatLng

    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      polygons,
      crs,
      from_date = "2020-11-05T00:00:00+00:00",
      to_date = "2020-11-05T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startCard4LBatchProcessesForSparsePolygons(): Unit = {
    val requestGroupUuid = UUID.randomUUID().toString

    val bboxLeft = Extent(3.841524124145508, 51.10796801619954, 3.842382431030273, 51.10850690517489)
    val bboxRight = Extent(7.5948143005371085, 51.475449262310086, 7.595586776733398, 51.47598385555211)

    val polygons = Array(bboxLeft, bboxRight)
      .map(bbox => MultiPolygon(Seq(bbox.toPolygon())))

    val crs = LatLng

    val batchRequestIds = batchProcessingService.start_card4l_batch_processes(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      polygons,
      crs,
      from_date = "2020-11-05T00:00:00+00:00",
      to_date = "2020-11-05T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      dem_instance = null,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      subfolder = requestGroupUuid,
      requestGroupUuid
    )

    println(s"batch process(es) $batchRequestIds will write to ${batchProcessingService.bucketName}/$requestGroupUuid")

    println(awaitDone(batchRequestIds.asScala))
  }

  @Test(expected = classOf[BatchProcessingService.NoSuchFeaturesException])
  def startBatchProcessForPeculiarTimeInterval(): Unit = {
    batchProcessingService.start_batch_process(
      collection_id = "sentinel-2-l2a",
      dataset_id = "sentinel-2-l2a",
      bbox = Extent(11.89, 41.58, 12.56, 42.2),
      bbox_srs = "EPSG:4326",
      from_date = "2021-12-07T00:00:00+00:00",
      to_date = "2021-12-06T00:00:00+00:00",
      band_names = Arrays.asList("B8A", "B11", "B05"),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      processing_options = Collections.emptyMap[String, Any]
    )
  }

  @Ignore("not implemented yet")
  @Test(expected = classOf[BatchProcessingService.NoSuchFeaturesException])
  def startCard4LBatchProcessesForXXX(): Unit = {
    val requestGroupUuid = UUID.randomUUID().toString

    batchProcessingService.start_card4l_batch_processes(
      collection_id = "sentinel-1-grd",
      dataset_id = "sentinel-1-grd",
      bbox = Extent(11.016694, 46.538743, 11.28595, 46.745225),
      bbox_srs = "EPSG:4326",
      from_date = "2018-07-01T00:00:00+00:00",
      to_date = "2018-07-03T00:00:00+00:00",
      band_names = Arrays.asList("VV", "VH", "HV", "HH"),
      dem_instance = "COPERNICUS_30",
      metadata_properties = Collections.emptyMap[String, JMap[String, Any]],
      subfolder = requestGroupUuid,
      requestGroupUuid
    )
  }

  private def awaitDone(batchRequestIds: Iterable[String],
                        batchProcessingService: BatchProcessingService = this.batchProcessingService): Map[String, String] = {
    import java.util.concurrent.TimeUnit._

    while (true) {
      SECONDS.sleep(10)
      val statuses = batchRequestIds.map(id => id -> batchProcessingService.get_batch_process_status(id)).toMap
      println(s"[${LocalTime.now()}] intermediary statuses: $statuses")

      val uniqueStatuses = statuses.values.toSet

      if (uniqueStatuses == Set("DONE") || uniqueStatuses.contains("FAILED")) {
        return statuses
      }
    }

    throw new AssertionError
  }
}
