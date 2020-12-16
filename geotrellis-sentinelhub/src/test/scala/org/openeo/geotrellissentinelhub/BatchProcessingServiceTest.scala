package org.openeo.geotrellissentinelhub

import geotrellis.vector.Extent
import org.junit.Assert.assertEquals
import org.junit.{Ignore, Test}

import java.util

class BatchProcessingServiceTest {
  private val batchProcessingService = new BatchProcessingService(bucketName = "openeo-sentinelhub",
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
      SampleType.FLOAT32
    )

    println(batchRequestId)
  }

  @Test
  def getBatchProcessStatus(): Unit = {
    val status = batchProcessingService.get_batch_process_status(batchRequestId = "8e1f83d5-1a65-4de3-9430-ba0435533647")

    assertEquals("DONE", status)
  }
}
