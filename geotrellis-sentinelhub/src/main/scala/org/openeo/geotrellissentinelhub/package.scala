package org.openeo

import java.io.InputStream
import java.io.StringWriter
import java.lang.Math.pow
import java.lang.Math.random
import java.net.SocketTimeoutException
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.util
import java.util.concurrent.TimeUnit.SECONDS

import scala.io.Source
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.MultibandTile
import geotrellis.vector.ProjectedExtent
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpStatusException}
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.openeo.geotrellissentinelhub.SampleType.SampleType

import scala.annotation.tailrec

package object geotrellissentinelhub {
  // TODO: encapsulate this
  // TODO: clean up JSON construction/parsing
  
  private val logger = LoggerFactory.getLogger(getClass)

  // TODO: invalidate key on 401 Unauthorized
  private val authTokenCache = CacheBuilder
    .newBuilder()
    .expireAfterWrite(1800L, SECONDS)
    .build(new CacheLoader[(String, String), String] {
      override def load(credentials: (String, String)): String = credentials match {
        case (clientId, clientSecret) => retry(5, clientId) { retrieveAuthToken(clientId, clientSecret) }
      }
    })

  // TODO: use AuthApi
  private def retrieveAuthToken(clientId: String, clientSecret: String): String = {
    val getAuthToken = Http("https://services.sentinel-hub.com/oauth/token")
      .postForm(Seq(
        "grant_type" -> "client_credentials",
        "client_id" -> clientId,
        "client_secret" -> clientSecret
      ))
      .asString
      .throwError

    val response = new ObjectMapper()
      .readValue[util.Map[String, Object]](getAuthToken.body, new TypeReference[util.Map[String, Object]](){})

    val token = response.get("access_token").asInstanceOf[String]
    logger.debug("received a new access token")

    token
  }

  def retrieveTileFromSentinelHub(datasetId: String, projectedExtent: ProjectedExtent, date: ZonedDateTime, width: Int,
                                  height: Int, bandNames: Seq[String], sampleType: SampleType, clientId: String,
                                  clientSecret: String): MultibandTile = {
    val ProjectedExtent(extent, crs) = projectedExtent
    val epsgCode = crs.epsgCode.getOrElse(s"unsupported crs $crs")

    val evalscript = s"""//VERSION=3
      function setup() {
        return {
          input: [{
            "bands": [${bandNames.map(bandName => s""""$bandName"""") mkString ", "}],
            "units": "DN"
          }],
          output: {
            bands: ${bandNames.size},
            sampleType: "$sampleType",
          }
        };
      }

      function evaluatePixel(sample) {
        return [${bandNames.map(bandName => s"sample.$bandName") mkString ", "}];
      }
    """
    val jsonFactory = new JsonFactory();
    val stringWriter = new StringWriter();
    val json = jsonFactory.createGenerator(stringWriter);
    json.writeString(evalscript);
    json.flush
    val evalscriptJson = stringWriter.toString()

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
            "type": "${datasetId}",
            "dataFilter": {
              "timeRange": {
                "from": "${date.format(ISO_INSTANT)}",
                "to": "${date.plusDays(1).format(ISO_INSTANT)}"
              }
            }
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
      "evalscript": ${evalscriptJson}
    }"""
    logger.info(s"JSON data for Sentinel Hub Process API: ${jsonData}")

    val url = "https://services.sentinel-hub.com/api/v1/process"
    val request = Http(url)
      .header("Content-Type", "application/json")
      .header("Authorization", s"Bearer ${authTokenCache.get((clientId, clientSecret))}")
      .header("Accept", "*/*")
      .postData(jsonData)

    logger.info(s"Executing request: ${request.urlBuilder(request)}")
    
    val response = retry(5, s"$date + $extent") {
      request.exec(parser = (code: Int, header: Map[String, IndexedSeq[String]], in: InputStream) =>
        if (code == 200)
          GeoTiffReader.readMultiband(IOUtils.toByteArray(in))
        else {
          val textBody = Source.fromInputStream(in, "utf-8").mkString
          throw HttpStatusException(code, header.get("Status").flatMap(_.headOption).getOrElse("UNKNOWN"), textBody)
        }
      )
    }

    response.body.tile
      .toArrayTile()
      // unless handled differently, NODATA pîxels are 0 according to
      // https://docs.sentinel-hub.com/api/latest/user-guides/datamask/#datamask---handling-of-pixels-with-no-data
      .mapBands { case (_, tile) => tile.withNoData(Some(0)) }
  }

  @tailrec
  private def retry[T](nb: Int, context: String, i: Int = 1)(fn: => T): T = {
    def retryable(e: Exception): Boolean = {
      val errorMessage = e match {
        case h: HttpStatusException => s"${h.getMessage}: ${h.body}"
        case _ => e.getMessage
      }

      logger.warn(s"Attempt $i failed: $context -> $errorMessage")

      i < nb && (e match {
        case h: HttpStatusException if h.code == 429 || h.code >= 500 => true
        case _: SocketTimeoutException => true
        case _ => false
      })
    }

    try
      return fn
    catch {
      case e: Exception if retryable(e) => Unit // won't recognize tail-recursive calls in a catch block as such
    }

    val exponentialRetryAfter = 1000 * pow(2, i - 1)
    val retryAfterWithJitter = (exponentialRetryAfter * (0.5 + random)).toInt
    Thread.sleep(retryAfterWithJitter)
    logger.info(s"Retry $i after ${retryAfterWithJitter}ms: $context")
    retry(nb, context, i + 1)(fn)
  }
}
