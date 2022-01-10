package org.openeo.geotrellis.stac

import org.slf4j.LoggerFactory

import java.io.IOException
import java.nio.file.{Files, Paths}
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

      Files.writeString(Paths.get(path), template)
    } catch {
      case e: IOException => logger.warn("Failed to write STAC metadata.", e)
    }

  }


}
