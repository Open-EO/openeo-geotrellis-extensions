package org.openeo.geotrellissentinelhub

import geotrellis.proj4.CRS
import geotrellis.vector.{Extent, ProjectedExtent}
import org.junit.Assert.{assertEquals, fail}
import org.junit.{Ignore, Test}
import scalaj.http.HttpStatusException

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME

class BatchProcessingApiTest {
  private val batchProcessingApi = new BatchProcessingApi

  private def accessToken: String = new AuthApi().authenticate(Utils.clientId, Utils.clientSecret).access_token

  @Ignore
  @Test
  def createBatchProcess(): Unit = {
    val dateTimes = Seq("2020-11-06T16:50:26Z", "2020-11-06T16:50:26Z", "2020-11-05T05:01:26Z", "2020-11-05T05:01:26Z")
      .map(ZonedDateTime.parse(_, ISO_OFFSET_DATE_TIME))

    try {
      val batchProcess = batchProcessingApi.createBatchProcess(
        datasetId = "S1GRD",
        boundingBox = ProjectedExtent(Extent(586240.0, 5350920.0, 588800.0, 5353480.0), CRS.fromEpsgCode(32633)),
        dateTimes,
        bandNames = Seq("VV", "VH"),
        SampleType.FLOAT32,
        bucketName = "openeo-sentinelhub",
        description = "BatchProcessingApiTest.createBatchProcess",
        accessToken
      )

      println(batchProcess.id)
    } catch {
      case e: HttpStatusException => fail(s"${e.statusLine}: ${e.body}")
    }
  }

  @Test
  def getBatchProcess(): Unit = {
    val batchProcess = batchProcessingApi.getBatchProcess("479cca6e-53d5-4477-ac5b-2c0ba8d3beba", accessToken)

    assertEquals("DONE", batchProcess.status)
  }

  @Ignore
  @Test
  def startBatchProcess(): Unit = {
    batchProcessingApi.startBatchProcess("479cca6e-53d5-4477-ac5b-2c0ba8d3beba", accessToken)
  }
}
