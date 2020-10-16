package org.openeo

import java.io.InputStream
import java.io.StringWriter
import java.lang.Math.pow
import java.lang.Math.random
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.util
import java.util.Scanner
import java.util.concurrent.TimeUnit.SECONDS

import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.{ArrayTile, FloatUserDefinedNoDataCellType, MultibandTile, Tile}
import geotrellis.vector.Extent
import org.apache.commons.io.IOUtils
import org.openeo.geotrellissentinelhub.bands.Band
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.cache.{CacheBuilder, CacheLoader}

import scala.annotation.tailrec

package object geotrellissentinelhub {
  
  private val logger = LoggerFactory.getLogger(getClass)
  private val authTokenCache = CacheBuilder // can be replaced with a memoizing supplier if we encapsulate this stuff
    .newBuilder()
    .expireAfterWrite(1800L, SECONDS)
    .build(new CacheLoader[(String, String), String] {
      override def load(credentials: (String, String)): String = credentials match {
        case (clientId, clientSecret) => retrieveAuthToken(clientId, clientSecret)
      }
    })

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

  def retrieveTileFromSentinelHub(datasetId: String, extent: Extent, date: ZonedDateTime, width: Int, height: Int,
                                  bands: Seq[_ <: Band], clientId: String, clientSecret: String): MultibandTile = {
    MultibandTile.apply(bands.map(retrieveTileFromSentinelHub(datasetId, extent, date, width, height, _, clientId, clientSecret)))
  }

  def retrieveTileFromSentinelHub(datasetId: String, extent: Extent, date: ZonedDateTime, width: Int, height: Int,
                                  band: Band, clientId: String, clientSecret: String): Tile = {
    // See: https://docs.sentinel-hub.com/api/latest/evalscript/v3/
    val nBands = 1
    val evalscript = s"""//VERSION=3
      function setup() {
        return {
          input: ["${band}"],
          output: {
            bands: ${nBands},
            sampleType: "FLOAT32",
          }
        };
      }

      function evaluatePixel(sample) {
        return [sample.${band}];
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
            "crs": "http://www.opengis.net/def/crs/EPSG/0/3857"
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
    
    try {
      retry(5, s"$date + $extent") {
        val response = request.exec(parser = 
          (code: Int, headers: Map[String, IndexedSeq[String]], inputStream: InputStream) => 
            if (code == 200) GeoTiffReader.singlebandGeoTiffReader.read(IOUtils.toByteArray(inputStream)) else toString(inputStream))

        if (response.isError)
          throw new RetryException(response)

        val tiff = response.body.asInstanceOf[SinglebandGeoTiff]

        tiff.tile.toArrayTile().withNoData(Some(1.0))
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Returning empty tile: $e")
        ArrayTile.empty(FloatUserDefinedNoDataCellType(1), width, height)
    }
  }

  private def toString(is:InputStream):String = {
    val result = new Scanner(is, "utf-8").useDelimiter("\\Z").next
    logger.warn(result)
    result
  }

  @tailrec
  private def retry[T](nb: Int, message: String, i: Int = 1)(fn: => T): T = {
    try {
      fn
    } catch {
      case e: Exception =>
        val exMessage = e match {
          case r: RetryException => s"${r.response.code}: ${r.response.header("Status").getOrElse("UNKNOWN")}"
          case _ => e.getMessage
        }
        logger.info(s"Attempt $i failed: $message -> $exMessage")
        if (i < nb) {
          val exponentialRetryAfter = 1000 * pow(2, i - 1)
          val retryAfterWithJitter = (exponentialRetryAfter * (0.5 + random)).toInt
          Thread.sleep(retryAfterWithJitter)
          logger.info(s"Retry $i after ${retryAfterWithJitter}ms: $message")
          retry(nb, message, i + 1)(fn)
        } else throw e
    }
  }

  class RetryException(val response: HttpResponse[Object]) extends Exception
}
