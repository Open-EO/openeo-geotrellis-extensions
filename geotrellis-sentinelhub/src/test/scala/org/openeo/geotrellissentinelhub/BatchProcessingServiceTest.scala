package org.openeo.geotrellissentinelhub

import geotrellis.vector.Extent
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

import java.util.{Arrays, Collections, UUID}
import java.time.LocalTime
import scala.collection.JavaConverters._

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
    val status = batchProcessingService.get_batch_process_status(batchRequestId = "7f3d98f2-4a9a-4fbe-adac-973f1cff5699")

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
}
