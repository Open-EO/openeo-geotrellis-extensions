package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import geotrellis.proj4.LatLng
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser.decode
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollectionMap
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.time.format.DateTimeFormatter.{ISO_INSTANT, ISO_OFFSET_DATE_TIME}
import java.time.{ZoneId, ZonedDateTime}

object CatalogApi {
  // TODO: remove in favor of existing JsonFeatureCollection
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

  def searchCard4L(collectionId: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime,
                   accessToken: String): Map[String, geotrellis.vector.Feature[Geometry, ZonedDateTime]] = {
    require(collectionId == "sentinel-1-grd", """only collection "sentinel-1-grd" is supported""")

    // TODO: reduce code duplication with dateTimes()
    val Extent(xmin, ymin, xmax, ymax) = boundingBox.reproject(LatLng)
    val lower = from.withZoneSameInstant(ZoneId.of("UTC"))
    val upper = to.withZoneSameInstant(ZoneId.of("UTC"))

    val requestBody =
      s"""
         |{
         |  "datetime": "${ISO_INSTANT format lower}/${ISO_INSTANT format upper}",
         |  "collections": ["$collectionId"],
         |  "query": {
         |    "sar:instrument_mode": {
         |      "eq": "IW"
         |    },
         |    "resolution": {
         |      "eq": "HIGH"
         |    },
         |    "polarization": {
         |      "eq": "DV"
         |    }
         |  },
         |  "bbox": [$xmin, $ymin, $xmax, $ymax]
         |}""".stripMargin

    val response = http(s"$endpoint/search", accessToken)
      .headers("Content-Type" -> "application/json")
      .postData(requestBody)
      .asString
      .throwError

    val featureCollection = decode[JsonFeatureCollectionMap](response.body)
      .valueOr(throw _)

    // it is assumed the returned geometries are in LatLng
    featureCollection.getAllMultiPolygonFeatures[Json]
      .mapValues(feature =>
        feature.mapData { properties =>
          val Some(datetime) = for {
            properties <- properties.asObject
            json <- properties("datetime")
            datetime <- json.asString
          } yield ZonedDateTime.parse(datetime, ISO_OFFSET_DATE_TIME)

          datetime
        })
  }

  private def http(url: String, accessToken: String): HttpRequest =
    Http(url)
      .option(HttpOptions.followRedirects(true))
      .headers("Authorization" -> s"Bearer $accessToken")
}
