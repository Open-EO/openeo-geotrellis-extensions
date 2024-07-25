package org.openeo.geotrellissentinelhub

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.raster.MultibandTile
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.ProjectedExtent
import net.jodah.failsafe.event.{ExecutionAttemptedEvent, ExecutionCompletedEvent, ExecutionScheduledEvent}
import net.jodah.failsafe.{ExecutionContext, Failsafe, RetryPolicy}
import org.apache.commons.io.IOUtils
import org.openeo.geotrelliscommon.CirceException
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpResponse}

import java.io.InputStream
import java.net.{SocketException, SocketTimeoutException, URI}
import java.time.{Duration, ZonedDateTime}
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.temporal.ChronoUnit.SECONDS
import java.util
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._
import scala.io.Source

trait ProcessApi {
  def getTile(datasetId: String, projectedExtent: ProjectedExtent, date: ZonedDateTime, width: Int, height: Int,
              bandNames: Seq[String], sampleType: SampleType, additionalDataFilters: util.Map[String, Any],
              processingOptions: util.Map[String, Any], accessToken: String): (MultibandTile, Double)
}

object DefaultProcessApi {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[DefaultProcessApi])
  private final val processingUnitsSpentHeader = "x-processingunits-spent"

  // TODO: made this package-private to be able to quickly test it
  private[geotrellissentinelhub] def withRetryAfterRetries[R](context: String)(fn: => R)(implicit logger: Logger): R = {
    val retryable: Throwable => Boolean = {
      case SentinelHubException(_, 400, _, responseBody) if responseBody.contains("Request body should be non-empty.") => true
      case SentinelHubException(_, statusCode, _, responseBody) if statusCode >= 500
        && !responseBody.contains("newLimit > capacity") && !responseBody.contains("Illegal request to https") => true
      case _: SocketTimeoutException => true
      case _: SocketException => true
      case _: CirceException => true
      case e => logger.error(s"Not attempting to retry unrecoverable error in context: $context", e); false
    }

    val shakyConnectionRetryPolicy = new RetryPolicy[R]()
      .handleIf(retryable.asJava)
      .withBackoff(1, 1000, SECONDS) // should not reach maxDelay because of maxAttempts 5
      .withJitter(0.5)
      .withMaxAttempts(5)
      .onFailedAttempt((attempt: ExecutionAttemptedEvent[R]) => {
        val e = attempt.getLastFailure
        logger.warn(s"Attempt ${attempt.getAttemptCount} failed in context: $context", e)
      })
      .onFailure((execution: ExecutionCompletedEvent[R]) => {
        val e = execution.getFailure
        logger.error(s"Failed after ${execution.getAttemptCount} attempt(s) in context: $context", e)
      })

    val isRateLimitingResponse: Throwable => Boolean = {
      case SentinelHubException(_, 429, _, _) => true
      case _ => false
    }

    val rateLimitingRetryPolicy = new RetryPolicy[R]()
      .handleIf(isRateLimitingResponse.asJava)
      .withMaxAttempts(5)
      .withDelay((_: R, sentinelHubException: SentinelHubException, _: ExecutionContext) => {
        val retryAfterHeader = "retry-after"

        val retryAfterSeconds = sentinelHubException
          .responseHeaders
          .find { case (header, _) => header equalsIgnoreCase retryAfterHeader }
          .map { case (_, values) => values.head.toLong }
          .getOrElse(throw new IllegalStateException(
            s"""missing expected header "$retryAfterHeader" (case-insensitive);""" +
              s" actual headers are: ${sentinelHubException.responseHeaders.keys mkString ", "}"))

        Duration.ofSeconds(retryAfterSeconds)
      })
      .onRetryScheduled((retry: ExecutionScheduledEvent[HttpResponse[MultibandGeoTiff]]) => {
        logger.warn(s"Scheduled retry within ${retry.getDelay} because of 429 response in context: $context")
      })

    Failsafe
      .`with`(util.Arrays.asList(shakyConnectionRetryPolicy, rateLimitingRetryPolicy))
      .get(() => fn)
  }
}

class DefaultProcessApi(endpoint: String, noDataValue: Double = 0) extends ProcessApi with Serializable {
  // TODO: clean up JSON construction/parsing
  import DefaultProcessApi._

  override def getTile(datasetId: String, projectedExtent: ProjectedExtent, date: ZonedDateTime, width: Int,
                       height: Int, bandNames: Seq[String], sampleType: SampleType,
                       additionalDataFilters: util.Map[String, Any],
                       processingOptions: util.Map[String, Any], accessToken: String): (MultibandTile, Double) = {
    val ProjectedExtent(extent, crs) = projectedExtent
    val epsgCode = crs.epsgCode.getOrElse(s"unsupported crs $crs")

    val dataFilter = {
      val timeRangeFilter = Map(
        "from" -> date.format(ISO_INSTANT),
        "to" -> date.plusDays(1).format(ISO_INSTANT)
      )

      additionalDataFilters.asScala
        .foldLeft(Map("timeRange" -> timeRangeFilter.asJava): Map[String, Any]) {_ + _}
        .asJava
    }

    val evalscript = {
      def bandValue(bandName: String): String =
        dnScaleFactor(datasetId, bandName)
          .map(value => s"sample.$bandName * $value").getOrElse(s"sample.$bandName")

      s"""//VERSION=3
         |function setup() {
         |  return {
         |    input: [{
         |      "bands": [${bandNames.map(bandName => s""""$bandName"""") mkString ", "}]
         |    }],
         |    output: {
         |      bands: ${bandNames.size},
         |      sampleType: "$sampleType",
         |    }
         |  };
         |}
         |
         |function evaluatePixel(sample) {
         |  return [${bandNames.map(bandValue) mkString ", "}];
         |}""".stripMargin
    }

    val objectMapper = new ObjectMapper

    val jsonData = s"""{
      "input": {
        "bounds": {
          "bbox": [${extent.xmin}, ${extent.ymin}, ${extent.xmax}, ${extent.ymax}],
          "properties": {
            "crs": "http://www.opengis.net/def/crs/EPSG/0/$epsgCode"
          }
        },
        "data": [
          {
            "type": "$datasetId",
            "dataFilter": ${objectMapper.writeValueAsString(dataFilter)},
            "processing": ${objectMapper.writeValueAsString(processingOptions)}
          }
        ]
      },
      "output": {
        "width": ${width.toString},
        "height": ${height.toString},
        "responses": [
          {
            "identifier": "default",
            "format": {
              "type": "image/tiff"
            }
          }
        ]
      },
      "evalscript": ${objectMapper.writeValueAsString(evalscript)}
    }"""

    logger.debug(s"JSON data for Sentinel Hub Process API: $jsonData")

    val url = URI.create(endpoint).resolve("/api/v1/process").toString
    val request = Http(url)
      .header("Content-Type", "application/json")
      .header("Authorization", s"Bearer $accessToken")
      .header("Accept", "*/*")
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 40000)
      .postData(jsonData)


    val context = s"getTile $datasetId $date"

    val response = withRetryAfterRetries(context) {
      request.exec(parser = (code: Int, headers: Map[String, IndexedSeq[String]], in: InputStream) =>
        if (code == 200) {
          val processingUnitsSpent = headers
            .get(processingUnitsSpentHeader)
            .flatMap(_.headOption)
            .map { pu =>
              logger.debug(s"$processingUnitsSpentHeader: $pu")
              pu.toDouble
            }
            .getOrElse {
              // Definition of a Processing Unit: https://docs.sentinel-hub.com/api/latest/api/overview/processing-unit/
              val pu = (width * height * bandNames.size / (512.0 * 512 * 3)) max 0.001
              logger.warn(s"$processingUnitsSpentHeader is missing, calculated $pu PU")
              pu
            }

          (GeoTiffReader.readMultiband(IOUtils.toByteArray(in)), processingUnitsSpent)
        } else {
          val textBody = Source.fromInputStream(in, "utf-8").mkString
          throw SentinelHubException(request, jsonData, code, headers, textBody)
        }
      )
    }

    val (multibandGeoTiff, processingUnitsSpent) = response.body

    (multibandGeoTiff.tile
      .toArrayTile()
      // unless handled differently, NODATA pîxels are 0 according to
      // https://docs.sentinel-hub.com/api/latest/user-guides/datamask/#datamask---handling-of-pixels-with-no-data
      .mapBands { case (_, tile) => tile.withNoData(Some(noDataValue)) }, processingUnitsSpent)
 }
}