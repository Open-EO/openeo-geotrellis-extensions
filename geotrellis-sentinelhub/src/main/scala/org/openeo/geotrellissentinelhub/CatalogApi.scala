package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import geotrellis.proj4.LatLng
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser.decode
import geotrellis.vector._
import geotrellis.vector.io.json.{JsonFeatureCollection, JsonFeatureCollectionMap}
import scalaj.http.{Http, HttpOptions, HttpRequest, HttpStatusException}

import java.net.URI
import java.time.format.DateTimeFormatter.{ISO_INSTANT, ISO_OFFSET_DATE_TIME}
import java.time.{ZoneId, ZonedDateTime}
import scala.collection.immutable.HashMap

object CatalogApi {
  private case class PagingContext(limit: Int, returned: Int, next: Option[Int])
  private case class PagedFeatureCollection(features: List[Json], context: PagingContext)
    extends JsonFeatureCollection(features)
  private case class PagedJsonFeatureCollectionMap(features: List[Json], context: PagingContext)
    extends JsonFeatureCollectionMap(features)
}

class CatalogApi(endpoint: String) {
  import CatalogApi._

  private val catalogEndpoint = URI.create(endpoint).resolve("/api/v1/catalog")

  // TODO: search distinct dates (https://docs.sentinel-hub.com/api/latest/api/catalog/examples/#search-with-distinct)?
  def dateTimes(collectionId: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime,
                accessToken: String, queryProperties: collection.Map[String, String] = Map()): Seq[ZonedDateTime] = {
    val Extent(xmin, ymin, xmax, ymax) = boundingBox.reproject(LatLng)
    val lower = from.withZoneSameInstant(ZoneId.of("UTC"))
    val upper = to.withZoneSameInstant(ZoneId.of("UTC"))

    val query = this.query(queryProperties)

    def getFeatureCollectionPage(nextToken: Option[Int]): PagedFeatureCollection = {
      val requestBody =
        s"""
           |{
           |    "bbox": [$xmin, $ymin, $xmax, $ymax],
           |    "datetime": "${ISO_INSTANT format lower}/${ISO_INSTANT format upper}",
           |    "collections": ["$collectionId"],
           |    "query": $query,
           |    "next": ${nextToken.orNull}
           |}""".stripMargin

      val request = http(s"$catalogEndpoint/search", accessToken)
        .headers("Content-Type" -> "application/json")
        .postData(requestBody)

      val response = request.asString

      if (response.isError) throw SentinelHubException(request, requestBody, response)

      decode[PagedFeatureCollection](response.body)
        .valueOr(throw _)
    }

    def getDateTimes(nextToken: Option[Int]): Seq[ZonedDateTime] = {
      val page = getFeatureCollectionPage(nextToken)

      val dateTimes = for {
        feature <- page.features.flatMap(_.asObject)
        properties <- feature("properties").flatMap(_.asObject)
        datetime <- properties("datetime").flatMap(_.asString)
      } yield ZonedDateTime.parse(datetime, ISO_OFFSET_DATE_TIME)

      page.context.next match {
        case None => dateTimes
        case nextToken => dateTimes ++ getDateTimes(nextToken)
      }
    }

    getDateTimes(nextToken = None)
  }

  def searchCard4L(collectionId: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime,
                   accessToken: String, queryProperties: collection.Map[String, String] = Map()):
  Map[String, geotrellis.vector.Feature[Geometry, ZonedDateTime]] = {
    require(collectionId == "sentinel-1-grd", """only collection "sentinel-1-grd" is supported""")

    // TODO: reduce code duplication with dateTimes()
    val Extent(xmin, ymin, xmax, ymax) = boundingBox.reproject(LatLng)
    val lower = from.withZoneSameInstant(ZoneId.of("UTC"))
    val upper = to.withZoneSameInstant(ZoneId.of("UTC"))

    val query = {
      val requiredProperties = HashMap(
        "sar:instrument_mode" -> "IW",
        "resolution" -> "HIGH",
        "polarization" -> "DV"
      )

      val allProperties = requiredProperties.merged(HashMap(queryProperties.toSeq: _*)) {
        case (requiredProperty @ (property, requiredValue), (_, overriddenValue)) =>
          if (overriddenValue == requiredValue) requiredProperty
          else throw new IllegalArgumentException(
            s"cannot override property $property value $requiredValue with $overriddenValue")
      }

      this.query(allProperties)
    }

    def getFeatureCollectionPage(nextToken: Option[Int]): PagedJsonFeatureCollectionMap = {
      val requestBody =
        s"""
           |{
           |  "datetime": "${ISO_INSTANT format lower}/${ISO_INSTANT format upper}",
           |  "collections": ["$collectionId"],
           |  "query": $query,
           |  "bbox": [$xmin, $ymin, $xmax, $ymax],
           |  "next": ${nextToken.orNull}
           |}""".stripMargin

      val request = http(s"$catalogEndpoint/search", accessToken)
        .headers("Content-Type" -> "application/json")
        .postData(requestBody)

      val response = request.asString

      if (response.isError) throw SentinelHubException(request, requestBody, response)

      decode[PagedJsonFeatureCollectionMap](response.body)
        .valueOr(throw _)
    }

    def getFeatures(nextToken: Option[Int]): Map[String, geotrellis.vector.Feature[Geometry, ZonedDateTime]] = {
      val page = getFeatureCollectionPage(nextToken)

      // it is assumed the returned geometries are in LatLng
      val features = page.getAllMultiPolygonFeatures[Json]
        .mapValues(feature =>
          feature.mapData { properties =>
            val Some(datetime) = for {
              properties <- properties.asObject
              json <- properties("datetime")
              datetime <- json.asString
            } yield ZonedDateTime.parse(datetime, ISO_OFFSET_DATE_TIME)

            datetime
          })

      page.context.next match {
        case None => features
        case nextToken => features ++ getFeatures(nextToken)
      }
    }

    getFeatures(nextToken = None)
  }

  private def http(url: String, accessToken: String): HttpRequest =
    Http(url)
      .option(HttpOptions.followRedirects(true))
      .headers("Authorization" -> s"Bearer $accessToken")

  private def query(queryProperties: collection.Map[String, String]): String =
    queryProperties
      .map { case (key, value) => s""""$key": {"eq": "$value"}"""}
      .mkString("{", ", ", "}")
}
