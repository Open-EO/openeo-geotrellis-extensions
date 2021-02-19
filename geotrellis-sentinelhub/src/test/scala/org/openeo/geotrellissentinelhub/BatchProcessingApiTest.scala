package org.openeo.geotrellissentinelhub

import geotrellis.proj4.CRS
import geotrellis.vector._
import org.junit.Assert.{assertEquals, fail}
import org.junit.{Ignore, Test}
import scalaj.http.HttpStatusException

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.util.UUID
import scala.collection.JavaConverters._

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
        processingOptions = Map(
          "backCoeff" -> "GAMMA0_ELLIPSOID",
          "orthorectify" -> false
        ).asJava,
        bucketName = "openeo-sentinelhub-vito-test",
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

  @Ignore
  @Test
  def createCard4LBatchProcess(): Unit = {
    val bounds = // the intersection of a feature with the initial bounding box
      """{
        |  "type":"Polygon",
        |  "coordinates":[
        |    [
        |      [
        |        35.715368104951445,
        |        -6.075694
        |      ],
        |      [
        |        35.7316723233983,
        |        -6.1452107540348715
        |      ],
        |      [
        |        35.75107377067866,
        |        -6.23476
        |      ],
        |      [
        |        35.861576,
        |        -6.23476
        |      ],
        |      [
        |        35.861576,
        |        -6.075694
        |      ],
        |      [
        |        35.715368104951445,
        |        -6.075694
        |      ]
        |    ]
        |  ]
        |}
        |""".stripMargin.parseGeoJson[Polygon]()

    val card4lId = UUID.randomUUID().toString

    val batchProcess = batchProcessingApi.createCard4LBatchProcess(
      datasetId = "S1GRD",
      bounds,
      dateTime = ZonedDateTime.parse("2021-02-15T15:54:57Z", ISO_OFFSET_DATE_TIME),
      bandNames = Seq("VH", "VV"),
      dataTakeId = "044CD7",
      card4lId,
      bucketName = "openeo-sentinelhub-vito-test", subFolder = card4lId, // FIXME: replace with "openeo-sentinelhub"
      accessToken
    )

    println(s"batch process ${batchProcess.id} will write to folder $card4lId")
  }
}
