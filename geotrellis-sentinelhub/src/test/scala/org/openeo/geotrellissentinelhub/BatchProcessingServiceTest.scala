package org.openeo.geotrellissentinelhub

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.vector.io.json.GeoJson
import geotrellis.vector._
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

import java.net.URL
import java.util
import java.util.{Arrays, Collections, UUID}
import java.time.LocalTime
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class BatchProcessingServiceTest {
  val endpoint = "https://services.sentinel-hub.com"
  private val batchProcessingService = new BatchProcessingService(endpoint, bucketName = "openeo-sentinelhub",
    Utils.clientId, Utils.clientSecret)

  @Ignore
  @Test
  def startBatchProcess(): Unit = {
    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-1-grd",
      dataset_id = "S1GRD",
      bbox = Extent(2.59003, 51.069, 2.8949, 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2019-10-10T00:00:00+00:00",
      to_date = "2019-10-10T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, Any],
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Ignore
  @Test
  def startBatchProcessForOrbitDirection(): Unit = {
    val batchRequestId = batchProcessingService.start_batch_process(
      collection_id = "sentinel-1-grd",
      dataset_id = "S1GRD",
      bbox = Extent(2.59003, 51.069, 2.8949, 51.2206),
      bbox_srs = "EPSG:4326",
      from_date = "2019-10-08T00:00:00+00:00",
      to_date = "2019-10-12T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      metadata_properties = Collections.singletonMap("orbitDirection", "ASCENDING"),
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  @Test
  def getBatchProcessStatus(): Unit = {
    val status =
      batchProcessingService.get_batch_process_status(batch_request_id = "7f3d98f2-4a9a-4fbe-adac-973f1cff5699")

    assertEquals("DONE", status)
  }

  @Ignore
  @Test
  def startCard4LBatchProcesses(): Unit = {
    val requestGroupId = UUID.randomUUID().toString

    val batchRequestIds = batchProcessingService.start_card4l_batch_processes(
      collection_id = "sentinel-1-grd",
      dataset_id = "S1GRD",
      bbox = Extent(35.666439, -6.23476, 35.861576, -6.075694),
      bbox_srs = "EPSG:4326",
      from_date = "2021-02-01T00:00:00+00:00",
      to_date = "2021-02-17T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV", "dataMask", "localIncidenceAngle"),
      dem_instance = null,
      metadata_properties = Collections.emptyMap[String, Any],
      subfolder = requestGroupId,
      requestGroupId
    )

    println(s"batch process(es) $batchRequestIds will write to ${batchProcessingService.bucketName}/$requestGroupId")

    println(awaitDone(batchRequestIds.asScala))

    new S3Service().download_stac_data(
      batchProcessingService.bucketName,
      requestGroupId,
      target_dir = "/tmp/saved_stac"
    )
  }

  @Ignore
  @Test
  def startCard4LBatchProcessesForOrbitDirection(): Unit = {
    val requestGroupId = UUID.randomUUID().toString

    val batchRequestIds = batchProcessingService.start_card4l_batch_processes(
      collection_id = "sentinel-1-grd",
      dataset_id = "S1GRD",
      bbox = Extent(35.666439, -6.23476, 35.861576, -6.075694),
      bbox_srs = "EPSG:4326",
      from_date = "2021-01-25T00:00:00+00:00",
      to_date = "2021-02-17T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV", "dataMask", "localIncidenceAngle"),
      dem_instance = null,
      metadata_properties = Collections.singletonMap("orbitDirection", "DESCENDING"),
      subfolder = requestGroupId,
      requestGroupId
    )

    println(s"batch process(es) $batchRequestIds will write to ${batchProcessingService.bucketName}/$requestGroupId")

    println(awaitDone(batchRequestIds.asScala))

    new S3Service().download_stac_data(
      batchProcessingService.bucketName,
      requestGroupId,
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
      dataset_id = "S1GRD",
      polygons,
      crs,
      from_date = "2020-11-05T00:00:00+00:00",
      to_date = "2020-11-05T00:00:00+00:00",
      band_names = Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      metadata_properties = Collections.emptyMap[String, Any],
      processing_options = Collections.emptyMap[String, Any]
    )

    println(awaitDone(Seq(batchRequestId)))
  }

  private def awaitDone(batchRequestIds: Iterable[String]): Map[String, String] = {
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

  @Test
  def sentinelHubTiles(): Unit = {
    val inputPolygons: Seq[Polygon] = Seq(Extent(3.494853973388672, 50.73767734908247, 3.515539169311523, 50.749436115275266).toPolygon())
    val gridTiles: util.ArrayList[Polygon] = new util.ArrayList[Polygon](util.Collections.singleton(Extent(3.5024070739746094, 50.7420770484489, 3.509659767150879, 50.74525435203266).toPolygon()))

    // TODO: generate gridTiles by collecting all 10km tiles within inputPolygons bbox
    val flobecq = readPolygon("/org/openeo/geotrellissentinelhub/flobecq.geojson")
    val waterloo = readPolygon("/org/openeo/geotrellissentinelhub/waterloo.geojson")

    val crs = CRS.fromEpsgCode(32631)
    val extent = GeometryCollection(Seq(flobecq, waterloo)).extent.reproject(LatLng, crs)
    val tileSize = 10000 // meter

    println(MultiPolygon(collectGridTiles(extent, tileSize).asScala).reproject(crs, LatLng).toGeoJson())

    println("---")

    val overlappingTiles = ArrayBuffer[Polygon]()

    println(gridTiles)
    println(overlappingTiles)

    for (polygon <- inputPolygons if !gridTiles.isEmpty) {
      val it = gridTiles.iterator()

      while (it.hasNext) {
        val tile = it.next()

        if (polygon intersects tile) {
          overlappingTiles += tile
          it.remove()
        }
      }
    }

    println(gridTiles)
    println(overlappingTiles)
  }

  private def collectGridTiles(extent: Extent, tileSize: Double): util.List[Polygon] = {
    val gridTiles = new util.ArrayList[Polygon]()

    for {
      upperLeftY <- extent.ymax until extent.ymin by -tileSize
      upperLeftX <- extent.xmin until extent.xmax by tileSize
    } {
      println(upperLeftX)
      gridTiles.add(Extent(upperLeftX, upperLeftY - tileSize, upperLeftX + tileSize, upperLeftY).toPolygon())
    }

    gridTiles
  }

  private def readPolygon(geoJsonClassPathResource: String): Polygon = {
    import scala.io.Source

    val in = Source.fromInputStream(getClass.getResourceAsStream(geoJsonClassPathResource))

    try GeoJson.parse[Polygon](in.mkString)
    finally in.close()
  }
}
