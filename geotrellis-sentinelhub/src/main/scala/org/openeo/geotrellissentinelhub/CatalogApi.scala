package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser.decode
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.time.format.DateTimeFormatter.{ISO_INSTANT, ISO_OFFSET_DATE_TIME}
import java.time.{ZoneId, ZonedDateTime}

object CatalogApi {
  private case class Feature(properties: Map[String, Json])
  private case class FeatureCollection(features: Array[Feature])
}

class CatalogApi {
  import CatalogApi._

  private val endpoint = "https://services.sentinel-hub.com/api/v1/catalog"

  def dateTimes(collectionId: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime,
                accessToken: String):
  Seq[ZonedDateTime] = {
    val Extent(xmin, ymin, xmax, ymax) = boundingBox.reproject(LatLng)
    val lower = from.withZoneSameInstant(ZoneId.of("UTC"))
    val upper = to.withZoneSameInstant(ZoneId.of("UTC"))

    val requestBody =
      s"""
         |{
         |    "bbox": [$xmin, $ymin, $xmax, $ymax],
         |    "datetime": "${ISO_INSTANT format lower}/${ISO_INSTANT format upper}",
         |    "collections": ["$collectionId"]
         |}""".stripMargin

    val response = http(s"$endpoint/search", accessToken)
      .headers("Content-Type" -> "application/json")
      .postData(requestBody)
      .asString
      .throwError

    val featureCollection = decode[FeatureCollection](response.body)
      .valueOr(throw _)

    for {
      feature <- featureCollection.features
      datetime <- feature.properties("datetime").asString
    } yield ZonedDateTime.parse(datetime, ISO_OFFSET_DATE_TIME)
  }

  private def http(url: String, accessToken: String): HttpRequest =
    Http(url)
      .option(HttpOptions.followRedirects(true))
      .headers("Authorization" -> s"Bearer $accessToken")
}
