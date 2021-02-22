package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import com.fasterxml.jackson.databind.ObjectMapper
import io.circe.generic.auto._
import io.circe.parser.decode
import io.circe.syntax._
import geotrellis.vector._
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.{BASIC_ISO_DATE, ISO_INSTANT}
import java.util

object BatchProcessingApi {
  private val logger = LoggerFactory.getLogger(classOf[BatchProcessingApi])

  private[geotrellissentinelhub] case class CreateBatchProcessResponse(id: String)
  private[geotrellissentinelhub] case class GetBatchProcessResponse(status: String)
}

class BatchProcessingApi {
  import BatchProcessingApi._

  private val endpoint = "https://services.sentinel-hub.com/api/v1/batch"

  private def http(url: String, accessToken: String): HttpRequest =
    Http(url)
      .option(HttpOptions.followRedirects(true))
      .headers("Authorization" -> s"Bearer $accessToken")

  def createBatchProcess(datasetId: String, boundingBox: ProjectedExtent, dateTimes: Seq[ZonedDateTime],
                         bandNames: Seq[String], sampleType: SampleType, processingOptions: util.Map[String, Any],
                         bucketName: String, description: String,
                         accessToken: String)
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
    val evalScript = this.evalScript(bandNames, identifiers, sampleType)

    // TODO: figure out how to work with heterogeneous types like Map[String, Any] in Circe
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
          |                    },
          |                    "processing": ${new ObjectMapper().writeValueAsString(processingOptions)}
          |                }
          |            ]
          |        },
          |        "output": {
          |            "responses": [${responses mkString ","}]
          |        },
          |        "evalscript": ${evalScript.asJson}
          |    },
          |    "tilingGrid": {
          |        "id": 1,
          |        "resolution": 10.0
          |    },
          |    "output": {
          |        "defaultTilePath": "s3://$bucketName",
          |        "cogOutput": true
          |    },
          |    "description": "$description"
          |}""".stripMargin

    logger.debug(requestBody)

    val response = http(s"$endpoint/process", accessToken)
      .headers("Content-Type" -> "application/json")
      .postData(requestBody)
      .asString
      .throwError

    decode[CreateBatchProcessResponse](response.body)
      .valueOr(throw _)
  }

  private def evalScript(bandNames: Seq[String], identifiers: Seq[String], sampleType: SampleType): String = {
    val outputs = identifiers map { identifier =>
      s"""|{
          |    id: "$identifier",
          |    bands: ${bandNames.size},
          |    sampleType: "$sampleType"
          |}""".stripMargin
    }

    val evaluatePixelReturnProperties = identifiers.zipWithIndex map { case (identifier, i) =>
      s"$identifier: bandValues(samples, scenes, $i)"
    }

    val quotedBandNames = bandNames.map(bandName => s""""$bandName"""")

    val bandValues = bandNames.map(bandName => s"samples[sampleIndex].$bandName")
    val noDataValues = bandNames.map(_ => "0")

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
        |    return sampleIndex >= 0 ? [${bandValues mkString ","}] : [${noDataValues mkString ","}]
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

  def getBatchProcess(batchRequestId: String, accessToken: String): GetBatchProcessResponse = {
    val response = http(s"$endpoint/process/$batchRequestId", accessToken)
      .headers("Authorization" -> s"Bearer $accessToken")
      .asString
      .throwError

    decode[GetBatchProcessResponse](response.body)
      .valueOr(throw _)
  }

  def startBatchProcess(batchRequestId: String, accessToken: String): Unit = {
    http(s"$endpoint/process/$batchRequestId/start", accessToken)
      .headers("Authorization" -> s"Bearer $accessToken")
      .postData("")
      .asString
      .throwError
  }

  def createCard4LBatchProcess(datasetId: String, bounds: Geometry, dateTime: ZonedDateTime, bandNames: Seq[String],
                               dataTakeId: String, card4lId: String, bucketName: String, subFolder: String,
                               accessToken: String): CreateBatchProcessResponse = {
    require(datasetId == "S1GRD", """only data set "S1GRD" is supported""")

    val (from, to) = (dateTime, dateTime plusSeconds 1)

    val (year, month, day) = (dateTime.getYear, dateTime.getMonthValue, dateTime.getDayOfMonth)
    val tilePath = f"s3://$bucketName/$subFolder/s1_rtc/<tileName>/$year/$month%02d/$day%02d/$dataTakeId/s1_rtc_${dataTakeId}_<tileName>_${year}_$month%02d_$day%02d_<outputId>.<format>"

    val evalScript = {
      val quotedBandNames = bandNames.map(bandName => s""""$bandName"""")
      val bandValues = bandNames.map(bandName => s"samples.$bandName")

      // TODO: specify nodataValue?
      // TODO: incorporate sampleType?
      s"""|//VERSION=3
          |function setup() {
          |  return {
          |    input: [{bands:[${quotedBandNames mkString ", "}], metadata: ["bounds"]}],
          |    output: [
          |      {
          |      id: "MULTIBAND",
          |      bands: ${quotedBandNames.size},
          |      sampleType: "FLOAT32",
          |      nodataValue: NaN
          |      }
          |    ]
          |  };
          |}
          |
          |function evaluatePixel(samples) {
          |  return {
          |    MULTIBAND: [${bandValues mkString ", "}]
          |  };
          |}
          |
          |function updateOutputMetadata(scenes, inputMetadata, outputMetadata) {
          |  outputMetadata.userData = {"tiles": scenes.tiles};
          |}""".stripMargin
    }

    // FIXME: replace tiling grid
    val requestBody =
      s"""|{
          |    "processRequest": {
          |        "input": {
          |            "bounds": {
          |                "geometry": ${bounds.toGeoJson()}
          |            },
          |            "data": [
          |                {
          |                    "type":"$datasetId",
          |                    "dataFilter": {
          |                        "timeRange": {
          |                            "from": "${ISO_INSTANT format from}",
          |                            "to": "${ISO_INSTANT format to}"
          |                        },
          |                        "acquisitionMode":"IW",
          |                        "polarization":"DV",
          |                        "resolution":"HIGH"
          |                    },
          |                    "processing": {
          |                        "backCoeff":"GAMMA0_TERRAIN",
          |                        "orthorectify":true,
          |                        "demInstance":"COPERNICUS_30",
          |                        "downsampling":"BILINEAR",
          |                        "upsampling":"BILINEAR"
          |                    }
          |                }
          |            ]
          |        },
          |        "output": {
          |            "responses": [
          |                {
          |                    "identifier": "MULTIBAND",
          |                    "format": {
          |                        "type": "image/tiff"
          |                    }
          |                },
          |                {
          |                    "identifier": "userdata",
          |                    "format": {
          |                        "type": "application/json"
          |                    }
          |                }
          |            ]
          |        },
          |        "evalscript": ${evalScript.asJson}
          |    },
          |    "tilingGrid": {
          |        "id": 3,
          |        "resolution": 0.0002
          |    },
          |    "output": {
          |        "defaultTilePath": "$tilePath",
          |        "cogOutput": true
          |    },
          |    "description": "card4lId: $card4lId"
          |}""".stripMargin

    logger.debug(requestBody)

    val response = http(s"$endpoint/process", accessToken)
      .headers("Content-Type" -> "application/json")
      .postData(requestBody)
      .asString
      .throwError

    decode[CreateBatchProcessResponse](response.body)
      .valueOr(throw _)
  }
}
