package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import geotrellis.vector.{Extent, ProjectedExtent}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser.decode
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.{BASIC_ISO_DATE, ISO_INSTANT}

object BatchProcessingApi {
  private val logger = LoggerFactory.getLogger(classOf[BatchProcessingApi])

  private[geotrellissentinelhub] case class CreateBatchProcessResponse(id: String)
  private[geotrellissentinelhub] case class GetBatchProcessResponse(status: String)
}

class BatchProcessingApi(clientId: String, clientSecret: String) {
  import BatchProcessingApi._

  private val endpoint = "https://services.sentinel-hub.com/api/v1/batch"
  private val authApi = new AuthApi

  private def http(url: String): HttpRequest = Http(url).option(HttpOptions.followRedirects(true))
  private def authToken: String = authApi.authenticate(clientId, clientSecret).access_token

  def createBatchProcess(datasetId: String, boundingBox: ProjectedExtent, dateTimes: Seq[ZonedDateTime],
                         bandNames: Seq[String], bucketName: String, description: String)
  : CreateBatchProcessResponse = {
    val ProjectedExtent(Extent(xmin, ymin, xmax, ymax), crs) = boundingBox
    val epsgCode = crs.epsgCode.getOrElse(s"unsupported crs $crs")

    val ascendingDateTimes = dateTimes
      .sortWith(_ isBefore _)

    val (from, to) = (ascendingDateTimes.head, ascendingDateTimes.last)

    val identifiers = ascendingDateTimes
      .map(_.toLocalDate)
      .distinct
      .map(date => s"_${BASIC_ISO_DATE format date}")

    val responses = this.responses(identifiers)
    val evalScript = this.evalScript(bandNames, identifiers)

    val requestBody =
      s"""|{
          |    "processRequest": {
          |        "input": {
          |            "bounds": {
          |                "bbox": [$xmin, $ymin, $xmax, $ymax],
          |                "properties": {
          |                    "crs": "http://www.opengis.net/def/crs/EPSG/0/$epsgCode"
          |                }
          |            },
          |            "data": [
          |                {
          |                    "type": "$datasetId",
          |                    "dataFilter": {
          |                        "timeRange": {
          |                            "from": "${ISO_INSTANT format from}",
          |                            "to": "${ISO_INSTANT format to}"
          |                        },
          |                        "mosaickingOrder": "leastRecent"
          |                    }
          |                }
          |            ]
          |        },
          |        "output": {
          |            "responses": [${responses mkString ","}]
          |        },
          |        "evalscript": ${Json.fromString(evalScript)}
          |    },
          |    "tilingGrid": {
          |        "id": 1,
          |        "resolution": 10.0
          |    },
          |    "bucketName": "$bucketName",
          |    "description": "$description"
          |}""".stripMargin

    logger.debug(requestBody)

    val response = http(s"$endpoint/process")
      .headers(
        "Authorization" -> s"Bearer $authToken", // TODO: put this in the http() method
        "Content-Type" -> "application/json"
      )
      .postData(requestBody)
      .asString
      .throwError

    decode[CreateBatchProcessResponse](response.body)
      .valueOr(throw _)
  }

  private def evalScript(bandNames: Seq[String], identifiers: Seq[String]): String = {
    val quotedBandNames = bandNames.map(bandName => s""""$bandName"""")

    val outputs = identifiers map { identifier =>
      s"""|{
          |    id: "$identifier",
          |    bands: 2,
          |    sampleType: "FLOAT32"
          |}""".stripMargin
    }

    val evaluatePixelReturnProperties = identifiers.zipWithIndex map { case (identifier, i) =>
      s"$identifier: bandValues(samples, scenes, $i)"
    }

    s"""|//VERSION=3
        |function setup() {
        |    return {
        |        input: [${quotedBandNames mkString ","}],
        |        output: [${outputs mkString ",\n"}],
        |        mosaicking: "ORBIT"
        |    };
        |}
        |
        |function evaluatePixel(samples, scenes) {
        |    return {
        |        ${evaluatePixelReturnProperties mkString ",\n"}
        |    };
        |}
        |
        |function bandValues(samples, scenes, sceneIdx) {
        |    function indexOf(sceneIdx) {
        |        return scenes.findIndex(scene => scene.idx === sceneIdx)
        |    }
        |
        |    let sampleIndex = indexOf(sceneIdx)
        |    return sampleIndex >= 0 ? [samples[sampleIndex].VV, samples[sampleIndex].VH] : [0, 0]
        |}
        |""".stripMargin
  }

  private def responses(identifiers: Seq[String]): Seq[String] = identifiers map { identifier =>
    s"""|{
        |    "identifier": "$identifier",
        |    "format": {
        |        "type": "image/tiff"
        |    }
        |}
        |""".stripMargin
  }

  def getBatchProcess(batchRequestId: String): GetBatchProcessResponse = {
    val response = http(s"$endpoint/process/$batchRequestId")
      .headers("Authorization" -> s"Bearer $authToken")
      .asString
      .throwError

    decode[GetBatchProcessResponse](response.body)
      .valueOr(throw _)
  }

  def startBatchProcess(batchRequestId: String): Unit = {
    http(s"$endpoint/process/$batchRequestId/start")
      .headers("Authorization" -> s"Bearer $authToken")
      .postData("")
      .asString
      .throwError
  }
}
