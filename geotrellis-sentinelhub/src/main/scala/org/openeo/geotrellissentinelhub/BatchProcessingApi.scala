package org.openeo.geotrellissentinelhub

import CirceException.decode
import cats.syntax.either._
import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.proj4.CRS
import io.circe.generic.auto._
import io.circe.syntax._
import geotrellis.vector._
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.net.URI
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.{BASIC_ISO_DATE, ISO_INSTANT}
import java.util
import scala.collection.JavaConverters._

object BatchProcessingApi {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[BatchProcessingApi])

  private[geotrellissentinelhub] case class CreateBatchProcessResponse(id: String)
  private[geotrellissentinelhub] case class GetBatchProcessResponse(status: String)
}

class BatchProcessingApi(endpoint: String) {
  import BatchProcessingApi._

  private val batchEndpoint = URI.create(endpoint).resolve("/api/v1/batch")

  private def http(url: String, accessToken: String): HttpRequest =
    Http(url)
      .option(HttpOptions.followRedirects(true))
      .headers("Authorization" -> s"Bearer $accessToken")

  def createBatchProcess(datasetId: String, boundingBox: ProjectedExtent, dateTimes: Seq[ZonedDateTime],
                         bandNames: Seq[String], sampleType: SampleType, additionalDataFilters: util.Map[String, Any],
                         processingOptions: util.Map[String, Any], bucketName: String, description: String,
                         accessToken: String, subfolder: String) : CreateBatchProcessResponse = {
    val multiPolygon = MultiPolygon(boundingBox.extent.toPolygon())
    val multiPolygonCrs = boundingBox.crs

    createBatchProcess(datasetId, multiPolygon, multiPolygonCrs, dateTimes, bandNames, sampleType,
      additionalDataFilters, processingOptions, bucketName, description, accessToken, subfolder)
  }

  def createBatchProcess(datasetId: String, boundingBox: ProjectedExtent, dateTimes: Seq[ZonedDateTime],
                         bandNames: Seq[String], sampleType: SampleType, additionalDataFilters: util.Map[String, Any],
                         processingOptions: util.Map[String, Any], bucketName: String, description: String,
                         accessToken: String) : CreateBatchProcessResponse =
    createBatchProcess(datasetId, boundingBox, dateTimes, bandNames, sampleType, additionalDataFilters,
      processingOptions, bucketName, description, accessToken, subfolder = null)

  def createBatchProcess(datasetId: String, multiPolygon: MultiPolygon, multiPolygonCrs: CRS, dateTimes: Seq[ZonedDateTime],
                         bandNames: Seq[String], sampleType: SampleType, additionalDataFilters: util.Map[String, Any],
                         processingOptions: util.Map[String, Any], bucketName: String, description: String,
                         accessToken: String) : CreateBatchProcessResponse =
    createBatchProcess(datasetId, multiPolygon, multiPolygonCrs, dateTimes, bandNames, sampleType,
      additionalDataFilters, processingOptions, bucketName, description, accessToken, subfolder = null)

  def createBatchProcess(datasetId: String, multiPolygon: MultiPolygon, multiPolygonCrs: CRS, dateTimes: Seq[ZonedDateTime],
                         bandNames: Seq[String], sampleType: SampleType, additionalDataFilters: util.Map[String, Any],
                         processingOptions: util.Map[String, Any], bucketName: String, description: String,
                         accessToken: String, subfolder: String) : CreateBatchProcessResponse =
    withRetries(context = s"createBatchProcess $datasetId") {
      require(dateTimes.nonEmpty)

      val epsgCode = multiPolygonCrs.epsgCode.getOrElse(s"unsupported crs $multiPolygonCrs")

      val ascendingDateTimes = dateTimes
        .sortWith(_ isBefore _)

      val (from, to) = (ascendingDateTimes.head, ascendingDateTimes.last)

      val tilePath = Option(subfolder)
        .map(f => s"s3://$bucketName/$f/<tileName>/<outputId>.<format>")
        .getOrElse(s"s3://$bucketName")

      val identifiers = ascendingDateTimes
        .map(_.toLocalDate)
        .distinct
        .map(date => s"_${BASIC_ISO_DATE format date}")

      val responses = this.responses(identifiers)
      val evalScript = this.evalScript(bandNames, identifiers, sampleType)

      val dataFilter: util.Map[String, Any] = {
        val baseDataFilters: Map[String, Any] = Map(
          "timeRange" -> Map(
            "from" -> ISO_INSTANT.format(from),
            "to" -> ISO_INSTANT.format(to)
          ).asJava,
          "mosaickingOrder" -> "leastRecent"
        )

        additionalDataFilters.asScala
          .foldLeft(baseDataFilters) {_ + _}
          .asJava
      }

      val objectMapper = new ObjectMapper()

      // TODO: figure out how to work with heterogeneous types like Map[String, Any] in Circe
      val requestBody =
        s"""|{
            |    "processRequest": {
            |        "input": {
            |            "bounds": {
            |                "geometry": ${multiPolygon.toGeoJson()},
            |                "properties": {
            |                    "crs": "http://www.opengis.net/def/crs/EPSG/0/$epsgCode"
            |                }
            |            },
            |            "data": [
            |                {
            |                    "type": "$datasetId",
            |                    "dataFilter": ${objectMapper.writeValueAsString(dataFilter)},
            |                    "processing": ${objectMapper.writeValueAsString(processingOptions)}
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
            |        "defaultTilePath": "$tilePath",
            |        "cogOutput": true
            |    },
            |    "description": "$description"
            |}""".stripMargin

      logger.debug(requestBody)

      val request = http(s"$batchEndpoint/process", accessToken)
        .headers("Content-Type" -> "application/json")
        .postData(requestBody)

      val response = request.asString

      if (response.isError) throw SentinelHubException(request, requestBody, response)

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
      s"$identifier: bandValues(samples, $i)"
    }

    val quotedBandNames = bandNames.map(bandName => s""""$bandName"""")

    val bandValues = bandNames.map(bandName => s"samples[sampleIndex].$bandName")
    val noDataValues = bandNames.map(_ => "0")

    s"""|//VERSION=3
        |function setup() {
        |    return {
        |        input: [{
        |          "bands": [${quotedBandNames mkString ","}],
        |          "units": "DN"
        |        }],
        |        output: [${outputs mkString ",\n"}],
        |        mosaicking: "ORBIT"
        |    };
        |}
        |
        |function evaluatePixel(samples) {
        |    return {
        |        ${evaluatePixelReturnProperties mkString ",\n"}
        |    };
        |}
        |
        |function bandValues(samples, sampleIndex) {
        |    return sampleIndex < samples.length ? [${bandValues mkString ","}] : [${noDataValues mkString ","}]
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

  def getBatchProcess(batchRequestId: String, accessToken: String): GetBatchProcessResponse =
    withRetries(context = s"getBatchProcess $batchRequestId") {
      val request = http(s"$batchEndpoint/process/$batchRequestId", accessToken)
      val response = request.asString

      if (response.isError) throw SentinelHubException(request, "", response)

      decode[GetBatchProcessResponse](response.body)
        .valueOr(throw _)
  }

  def startBatchProcess(batchRequestId: String, accessToken: String): Unit =
    withRetries(context = s"startBatchProcess $batchRequestId") {
      val requestBody = ""

      val request = http(s"$batchEndpoint/process/$batchRequestId/start", accessToken)
        .postData(requestBody)

      val response = request.execute()

      if (response.isError) throw SentinelHubException(request, requestBody, response)
  }

  def restartPartiallyFailedBatchProcess(batchRequestId: String, accessToken: String): Unit =
    withRetries(context = s"restartPartiallyFailedBatchProcess $batchRequestId") {
      val requestBody = ""

      val request = http(s"$batchEndpoint/process/$batchRequestId/restartpartial", accessToken)
        .postData(requestBody)

      val response = request.execute()

      if (response.isError) throw SentinelHubException(request, requestBody, response)
  }

  def createCard4LBatchProcess(datasetId: String, bounds: Geometry, dateTime: ZonedDateTime, bandNames: Seq[String],
                               dataTakeId: String, card4lId: String, demInstance: String,
                               additionalDataFilters: util.Map[String, Any], bucketName: String,
                               subFolder: String, accessToken: String): CreateBatchProcessResponse =
    withRetries(context = s"createCard4LBatchProcess $datasetId") {
      require(datasetId == "S1GRD", """only data set "S1GRD" is supported""")

      val (from, to) = (dateTime, dateTime plusSeconds 1)

      val (year, month, day) = (dateTime.getYear, dateTime.getMonthValue, dateTime.getDayOfMonth)
      val tilePath = f"s3://$bucketName/$subFolder/s1_rtc/<tileName>/$year/$month%02d/$day%02d/$dataTakeId/s1_rtc_${dataTakeId}_<tileName>_${year}_$month%02d_$day%02d_<outputId>.<format>"

      val processingOptions = {
        val requiredOptions = Map(
          "backCoeff" -> "GAMMA0_TERRAIN".asJson,
          "orthorectify" -> true.asJson,
          "downsampling" -> "BILINEAR".asJson,
          "upsampling" -> "BILINEAR".asJson
        )

        Option(demInstance).foldLeft(requiredOptions) { case (options, demInstance) =>
          options + ("demInstance" -> demInstance.asJson)
        }
      }

      val dataFilter: util.Map[String, Any] = {
        val baseDataFilters: Map[String, Any] = Map(
          "timeRange" -> Map(
            "from" -> ISO_INSTANT.format(from),
            "to" -> ISO_INSTANT.format(to)
          ).asJava,
          "acquisitionMode" -> "IW",
          "polarization" -> "DV",
          "resolution" -> "HIGH"
        )

        additionalDataFilters.asScala
          .foldLeft(baseDataFilters) {_ + _}
          .asJava
      }

      val evalScript = {
        val quotedBandNames = bandNames.map(bandName => s""""$bandName"""")
        val bandValues = bandNames.map(bandName => s"samples.$bandName")

        s"""|//VERSION=3
            |function setup() {
            |  return {
            |    input: [{bands:[${quotedBandNames mkString ", "}], metadata: ["bounds"]}],
            |    output: [
            |      {
            |      id: "MULTIBAND",
            |      bands: ${quotedBandNames.size},
            |      sampleType: "FLOAT32"
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
            |                    "dataFilter": ${new ObjectMapper().writeValueAsString(dataFilter)},
            |                    "processing": ${processingOptions.asJson}
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

      val request = http(s"$batchEndpoint/process", accessToken)
        .headers("Content-Type" -> "application/json")
        .postData(requestBody)

      val response = request.asString

      if (response.isError) throw SentinelHubException(request, requestBody, response)

      decode[CreateBatchProcessResponse](response.body)
        .valueOr(throw _)
    }
}
