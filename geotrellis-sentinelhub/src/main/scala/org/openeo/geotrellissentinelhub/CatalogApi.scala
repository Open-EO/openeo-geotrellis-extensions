package org.openeo.geotrellissentinelhub

import cats.syntax.either._
import com.fasterxml.jackson.databind.ObjectMapper
import geotrellis.proj4.{CRS, LatLng}
import io.circe.Json
import io.circe.generic.auto._
import geotrellis.vector._
import geotrellis.vector.io.json.{JsonFeatureCollection, JsonFeatureCollectionMap}
import org.openeo.geotrelliscommon.CirceException.decode
import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpOptions, HttpRequest}

import java.net.URI
import java.time.format.DateTimeFormatter.{ISO_INSTANT, ISO_OFFSET_DATE_TIME}
import java.time.{ZoneId, ZonedDateTime}
import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

trait CatalogApi {
  def dateTimes(collectionId: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime,
                accessToken: String,
                queryProperties: util.Map[String, util.Map[String, Any]] = util.Collections.emptyMap()): Seq[ZonedDateTime] = {
    val geometry = boundingBox.extent.toPolygon()
    val geometryCrs = boundingBox.crs

    dateTimes(collectionId, geometry, geometryCrs, from, to, accessToken, queryProperties)
  }

  def dateTimes(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime, to: ZonedDateTime,
                accessToken: String, queryProperties: util.Map[String, util.Map[String, Any]]): Seq[ZonedDateTime]

  def searchCard4L(collectionId: String, boundingBox: ProjectedExtent, from: ZonedDateTime, to: ZonedDateTime,
                   accessToken: String,
                   queryProperties: util.Map[String, util.Map[String, Any]] = util.Collections.emptyMap()):
  Map[String, geotrellis.vector.Feature[Geometry, FeatureData]] = {
    val geometry = boundingBox.extent.toPolygon()
    val geometryCrs = boundingBox.crs

    searchCard4L(collectionId, geometry, geometryCrs, from, to, accessToken, queryProperties)
  }

  def searchCard4L(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime, to: ZonedDateTime,
                   accessToken: String, queryProperties: util.Map[String, util.Map[String, Any]]):
  Map[String, geotrellis.vector.Feature[Geometry, FeatureData]] = {
    require(collectionId == "sentinel-1-grd", """only collection "sentinel-1-grd" is supported""")

    val requiredProperties = HashMap(
      "sar:instrument_mode" -> Map("eq" -> ("IW": Any)).asJava,
      "s1:resolution" -> Map("eq" -> ("HIGH": Any)).asJava,
      "s1:polarization" -> Map("eq" -> ("DV": Any)).asJava
    )

    val allProperties = requiredProperties.merged(HashMap(queryProperties.asScala.toSeq: _*)) {
      case (requiredProperty @ (property, requiredValue), (_, overriddenValue)) =>
        if (overriddenValue == requiredValue) requiredProperty
        else throw new IllegalArgumentException(
          s"cannot override property $property value $requiredValue with $overriddenValue")
    }

    search(collectionId, geometry, geometryCrs, from, to, accessToken, allProperties.asJava)
  }

  def search(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime, to: ZonedDateTime,
                   accessToken: String, queryProperties: util.Map[String, util.Map[String, Any]]):
  Map[String, geotrellis.vector.Feature[Geometry, FeatureData]]

}

object FeatureData {
  private val dateTimeFormatter = ISO_OFFSET_DATE_TIME

  def apply(dateTime: ZonedDateTime): FeatureData = new FeatureData(dateTime)
}

case class FeatureData(properties: Map[String, Json], selfUrl: Option[String] = None) {
  def this(dateTime: ZonedDateTime) =
    this(Map("datetime" -> Json.fromString(FeatureData.dateTimeFormatter format dateTime)))

  def dateTime: ZonedDateTime = {
    val Some(dateTime) = for {
      datetime <- properties("datetime").asString
    } yield ZonedDateTime.parse(datetime, FeatureData.dateTimeFormatter)

    dateTime
  }
}

object DefaultCatalogApi {
  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[DefaultCatalogApi])

  private final val maxLimit = 100 // as per the docs
  private val filterFormatter = new Cql2TextFormatter

  private case class PagingContext(limit: Int, returned: Int, next: Option[Int])
  private case class PagedFeatureCollection(features: List[Json], context: PagingContext)
    extends JsonFeatureCollection(features)
  private case class PagedJsonFeatureCollectionMap(features: List[Json], context: PagingContext)
    extends JsonFeatureCollectionMap(features)

  private def getSelfUrl(js: Json): Option[String] = {
    val cursor = js.hcursor
    // for-statement seems perfect to do a lot of Option checking
    for {
      links <- cursor.downField("links").values

      link <- links.find(j => (for {
        json <- j.asObject
        rel <- json.toMap.get("rel")
        s <- rel.asString
      } yield s == "self").getOrElse(false))

      json <- link.asObject
      href <- json.toMap.get("href")
      href <- href.asString
    } yield {
      href
    }
  }
}

class DefaultCatalogApi(endpoint: String) extends CatalogApi {
  import DefaultCatalogApi._

  private val catalogEndpoint = URI.create(endpoint).resolve("/api/v1/catalog/1.0.0")
  private val objectMapper = new ObjectMapper

  // TODO: search distinct dates (https://docs.sentinel-hub.com/api/latest/api/catalog/examples/#search-with-distinct)?
  override def dateTimes(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime, to: ZonedDateTime,
                accessToken: String, queryProperties: util.Map[String, util.Map[String, Any]]): Seq[ZonedDateTime] = {
    val lower = from.withZoneSameInstant(ZoneId.of("UTC"))
    val upper = to.withZoneSameInstant(ZoneId.of("UTC"))

    val filter = this.filter(queryProperties)

    def getFeatureCollectionPage(limit: Int, nextToken: Option[Int]): PagedFeatureCollection =
      withRetries(context = s"dateTimes $collectionId nextToken $nextToken") {
        val requestBody =
          s"""
             |{
             |    "intersects": ${geometry.reproject(geometryCrs, LatLng).toGeoJson()},
             |    "datetime": "${ISO_INSTANT format lower}/${ISO_INSTANT format upper}",
             |    "collections": ["$collectionId"],
             |    "filter": $filter,
             |    "limit": $limit,
             |    "next": ${nextToken.orNull}
             |}""".stripMargin

        logger.debug(s"JSON data for Sentinel Hub Catalog API: $requestBody")

        val request = http(s"$catalogEndpoint/search", accessToken)
          .headers("Content-Type" -> "application/json")
          .postData(requestBody)

        val response = request.asString

        if (response.isError) throw SentinelHubException(request, requestBody, response)

        decode[PagedFeatureCollection](response.body)
          .valueOr(throw _)
    }

    def getDateTimes(limit: Int, nextToken: Option[Int]): Seq[ZonedDateTime] = {
      val page = getFeatureCollectionPage(limit, nextToken)

      val dateTimes = for {
        feature <- page.features.flatMap(_.asObject)
        properties <- feature("properties").flatMap(_.asObject)
        datetime <- properties("datetime").flatMap(_.asString)
      } yield ZonedDateTime.parse(datetime, ISO_OFFSET_DATE_TIME)

      page.context.next match {
        case None => dateTimes
        case nextToken => dateTimes ++ getDateTimes(page.context.limit, nextToken)
      }
    }

    getDateTimes(limit = maxLimit, nextToken = None)
  }

  override def search(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime,
                      to: ZonedDateTime, accessToken: String, queryProperties: util.Map[String, util.Map[String, Any]]):
  Map[String, geotrellis.vector.Feature[Geometry, FeatureData]] =
    withRetries(context = s"search $collectionId from $from to $to for ${geometry.getNumGeometries} geometries") {
      // TODO: reduce code duplication with dateTimes()
      val lower = from.withZoneSameInstant(ZoneId.of("UTC"))
      val upper = to.withZoneSameInstant(ZoneId.of("UTC"))

      val filter = this.filter(queryProperties)

      def getFeatureCollectionPage(limit: Int, nextToken: Option[Int]): PagedJsonFeatureCollectionMap = {
        val requestBody =
          s"""
             |{
             |  "datetime": "${ISO_INSTANT format lower}/${ISO_INSTANT format upper}",
             |  "collections": ["$collectionId"],
             |  "filter": $filter,
             |  "intersects": ${geometry.reproject(geometryCrs, LatLng).toGeoJson()},
             |  "limit": $limit,
             |  "next": ${nextToken.orNull}
             |}""".stripMargin

        logger.debug(s"JSON data for Sentinel Hub Catalog API: $requestBody")

        val request = http(s"$catalogEndpoint/search", accessToken)
          .headers("Content-Type" -> "application/json")
          .postData(requestBody)

        val response = request.asString

        if (response.isError) throw SentinelHubException(request, requestBody, response)

        decode[PagedJsonFeatureCollectionMap](response.body)
          .valueOr(throw _)
      }


      def getFeatures(limit: Int, nextToken: Option[Int]): Map[String, geotrellis.vector.Feature[Geometry, FeatureData]] = {
        val page = getFeatureCollectionPage(limit, nextToken)

        // it is assumed the returned geometries are in LatLng
        val features: Map[String, Feature[Geometry, FeatureData]] = page.getAll[Json]
          .flatMap { case (id, featureJson) =>
            val selfUrl = getSelfUrl(featureJson)
            for {
              feature <- featureJson.as[Feature[Geometry, Json]].toOption
              properties <- feature.data.asObject
            } yield id -> Feature(feature.geom, FeatureData(properties.toMap, selfUrl))
          }

        page.context.next match {
          case None => features
          case nextToken => features ++ getFeatures(page.context.limit, nextToken)
        }
      }

      getFeatures(limit = maxLimit, nextToken = None)
  }

  private def http(url: String, accessToken: String): HttpRequest =
    Http(url)
      .option(HttpOptions.followRedirects(true))
      .headers("Authorization" -> s"Bearer $accessToken")
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 40000)

  private def filter(queryProperties: util.Map[String, util.Map[String, Any]]): String = {
    if (queryProperties.isEmpty) null
    else objectMapper.writeValueAsString(filterFormatter.format(queryProperties))
  }
}

/**
 * For lack of a better name, a CatalogApi implementation that returns features that exactly cover the input date range
 * and geometry; it simplifies working with data that is not available in the real Catalog API like the Mapzen DEM.
 */
class MadeToMeasureCatalogApi extends CatalogApi {
  override def dateTimes(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime,
                         to: ZonedDateTime, accessToken: String,
                         queryProperties: util.Map[String, util.Map[String, Any]]): Seq[ZonedDateTime] =
    sequentialDays(from, to)

  override def search(collectionId: String, geometry: Geometry, geometryCrs: CRS, from: ZonedDateTime,
                      to: ZonedDateTime, accessToken: String, queryProperties: util.Map[String, util.Map[String, Any]]):
  Map[String, Feature[Geometry, FeatureData]] = {
    val features = for ((timestamp, index) <- sequentialDays(from, to).zipWithIndex)
      yield index.toString -> Feature(geometry.reproject(geometryCrs, LatLng),
        FeatureData(Map("datetime" -> Json.fromString(timestamp format ISO_OFFSET_DATE_TIME)), None))

    features.toMap
  }
}

class Cql2TextFormatter {
  def format(queryProperties: util.Map[String, util.Map[String, Any]]): String = {
    val cql2Texts = for {
      (property, criteria) <- queryProperties.asScala
      (operator, value) <- criteria.asScala
    } yield formatCql2Text(property, operator, value)

    cql2Texts mkString " and "
  }

  private def formatCql2Text(property: String, operator: String, value: Any): String = {
    val cql2TextOperator = operator match {
      case "eq" => "="
      case "neq" => "<>"
      case "lt" => "<"
      case "lte" => "<="
      case "gt" => ">"
      case "gte" => ">="
      case _ => throw new IllegalArgumentException(s"unsupported operator $operator")
    }

    val cql2TextValue = if (value.isInstanceOf[String]) s"'$value'" else value.toString

    s"$property $cql2TextOperator $cql2TextValue"
  }
}
