package org.openeo.geotrellis.stac

import org.openeo.geotrellis.geotiff.uploadToS3
import org.openeo.geotrellis.getTempFile
import org.slf4j.LoggerFactory

import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.{Files, Paths, StandardCopyOption, StandardOpenOption}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID.randomUUID

class STACItem {

  private val logger = LoggerFactory.getLogger(classOf[STACItem])

  private var href: String = _

  def asset(href: String, mimeType: String = "image/tiff; application=geotiff; profile=cloud-optimized", title: String = "openEO result") = {
    this.href = href
  }

  def write(path: String): Unit = {
    val now = ZonedDateTime.now()
    val template =
      f"""
         |{
         |  "stac_version": "1.0.0",
         |  "stac_extensions": [],
         |  "type": "Feature",
         |  "id": "${randomUUID().toString}",
         |  "properties": {
         |    "title": "Core Item",
         |    "description": "STAC item for openEO batch result",
         |    "created": "${DateTimeFormatter.ISO_ZONED_DATE_TIME.format(now)}",
         |    "updated": "${DateTimeFormatter.ISO_ZONED_DATE_TIME.format(now)}"
         |  },
         |  "links": [
         |    {
         |      "rel": "collection",
         |      "href": "./collection.json",
         |      "type": "application/json",
         |      "title": "openEO batch job collection"
         |    }
         |  ],
         |  "assets": {
         |    "result": {
         |      "href": "${href}",
         |      "type": "image/tiff; application=geotiff; profile=cloud-optimized",
         |      "roles": [
         |        "data"
         |      ]
         |    }
         |
         |  }
         |}
         |""".stripMargin
    try {

      val tempFile = getTempFile(null, ".json")
      Files.write(tempFile, template.getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE_NEW)

      if (path.startsWith("s3:/")) {
        val correctS3Path = path.replaceFirst("s3:/(?!/)", "s3://")
        uploadToS3(tempFile, correctS3Path)
        Files.delete(tempFile)
      } else {
        Files.move(tempFile, Paths.get(path), StandardCopyOption.REPLACE_EXISTING)
      }

    } catch {
      case e: IOException => logger.warn("Failed to write STAC metadata.", e)
    }

  }


}
