package org.openeo

import java.io.InputStream
import java.lang.Math.pow
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.util.Scanner

import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.{ArrayTile, FloatUserDefinedNoDataCellType, MultibandTile, Tile}
import geotrellis.vector.Extent
import org.apache.commons.io.IOUtils
import org.openeo.geotrellissentinelhub.bands.Band
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}

import scala.annotation.tailrec

package object geotrellissentinelhub {
  
  val logger = LoggerFactory.getLogger(getClass)

  def retrieveTileFromSentinelHub(uuid: String, endpoint: String, extent: Extent, date: ZonedDateTime, width: Int, height: Int, bands: Seq[_ <: Band]): MultibandTile = {
    MultibandTile.apply(bands.map(retrieveTileFromSentinelHub(uuid, endpoint, extent, date, width, height, _)))
  }

  def retrieveTileFromSentinelHub(uuid: String, endpoint: String, extent: Extent, date: ZonedDateTime, width: Int, height: Int, band: Band): Tile = {
    val url = s"$endpoint/ogc/wcs/$uuid"

    val request = Http(url)
      .param("service", "WCS")
      .param("request", "getCoverage")
      .param("format", "image/tiff;depth=32f")
      .param("width", width.toString)
      .param("height", height.toString)
      .param("coverage", band.toString)
      .param("time", date.format(ISO_DATE_TIME) + "/" + date.plusDays(1).format(ISO_DATE_TIME))
      .param("bbox", extent.xmin + "," + extent.ymin + "," + extent.xmax + "," + extent.ymax)
      .param("crs", "EPSG:3857")
    
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
          val exponentialRetryAfter = 1000 * pow(2, i - 1).toInt
          Thread.sleep(exponentialRetryAfter)
          logger.info(s"Retry $i after ${exponentialRetryAfter}ms: $message")
          retry(nb, message, i + 1)(fn)
        } else throw e
    }
  }

  class RetryException(val response: HttpResponse[Object]) extends Exception
}
