package org.openeo.geotrellissentinelhub

import geotrellis.vector.Extent
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

import java.util
import java.util.UUID

class BatchProcessingServiceTest {
  private val batchProcessingService = new BatchProcessingService(bucketName = "openeo-sentinelhub-vito-test", // FIXME: restore to "openeo-sentinelhub"
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
      band_names = util.Arrays.asList("VH", "VV"),
      SampleType.FLOAT32,
      processing_options = util.Collections.emptyMap[String, Any]
    )

    println(batchRequestId)
  }

  @Test
  def getBatchProcessStatus(): Unit = {
    val status = batchProcessingService.get_batch_process_status(batchRequestId = "7f3d98f2-4a9a-4fbe-adac-973f1cff5699")

    assertEquals("DONE", status)
  }

  @Ignore
  @Test
  def startCard4LBatchProcesses(): Unit = {
    val requestGroupId = UUID.randomUUID().toString // OpenEO batch job ID is probably a good candidate

    val batchRequestIds = batchProcessingService.start_card4l_batch_processes(
      collection_id = "sentinel-1-grd",
      dataset_id = "S1GRD",
      bbox = Extent(35.666439, -6.23476, 35.861576, -6.075694),
      bbox_srs = "EPSG:4326",
      from_date = "2021-02-01T00:00:00+00:00",
      to_date = "2021-02-17T00:00:00+00:00",
      band_names = util.Arrays.asList("VH", "VV"),
      subfolder = requestGroupId,
      requestGroupId
    )

    println(s"batch process(es) ${batchRequestIds.mkString("[", ", ", "]")} will write to folder $requestGroupId")
  }
}
