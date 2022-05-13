package org.openeo.geotrellissentinelhub

import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.raster.MultibandTile
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.vector.ProjectedExtent
import org.apache.commons.io.IOUtils
import org.openeo.geotrelliscommon.BatchJobMetadataTracker
import org.openeo.geotrellissentinelhub.SampleType.SampleType
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.Http

import java.io.InputStream
import java.net.URI
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.util
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.io.Source

trait ProcessApi {
  def getTile(datasetId: String, projectedExtent: ProjectedExtent, date: ZonedDateTime, width: Int, height: Int,
              bandNames: Seq[String], sampleType: SampleType, additionalDataFilters: util.Map[String, Any],
              processingOptions: util.Map[String, Any], accessToken: String): MultibandTile
}

object DefaultProcessApi {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[DefaultProcessApi])
}

class DefaultProcessApi(endpoint: String) extends ProcessApi with Serializable {
  // TODO: clean up JSON construction/parsing
  import DefaultProcessApi._

  override def getTile(datasetId: String, projectedExtent: ProjectedExtent, date: ZonedDateTime, width: Int,
                       height: Int, bandNames: Seq[String], sampleType: SampleType,
                       additionalDataFilters: util.Map[String, Any],
                       processingOptions: util.Map[String, Any], accessToken: String): MultibandTile = {
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
      .timeout(connTimeoutMs = 1000, readTimeoutMs = 40000)
      .postData(jsonData)

    val response = withRetries(context = s"getTile $datasetId $date") {
      request.exec(parser = (code: Int, header: Map[String, IndexedSeq[String]], in: InputStream) =>
        if (code == 200) {
          val tracker = BatchJobMetadataTracker.tracker("")
          val pUnitsSpent = header
            .get("x-processingunits-spent").flatMap(_.headOption)
            .getOrElse(math.max(0.001,width*height*bandNames.size/(512.0*512.0*3.0)).toString).toDouble
          tracker.registerDoubleCounter(BatchJobMetadataTracker.SH_PU)
          tracker.add(BatchJobMetadataTracker.SH_PU, pUnitsSpent)
          GeoTiffReader.readMultiband(IOUtils.toByteArray(in))
        }
        else {
          val textBody = Source.fromInputStream(in, "utf-8").mkString
          throw SentinelHubException(request, jsonData, code,
            statusLine = header.get("Status").flatMap(_.headOption).getOrElse("UNKNOWN"), textBody)
        }
      )
    }

    response.body.tile
      .toArrayTile()
      // unless handled differently, NODATA pîxels are 0 according to
      // https://docs.sentinel-hub.com/api/latest/user-guides/datamask/#datamask---handling-of-pixels-with-no-data
      .mapBands { case (_, tile) => tile.withNoData(Some(0)) }
 }
}