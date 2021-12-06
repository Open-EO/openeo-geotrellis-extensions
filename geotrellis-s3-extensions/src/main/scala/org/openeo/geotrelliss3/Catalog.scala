package org.openeo.geotrelliss3

import java.time.format.DateTimeFormatter.ofPattern
import java.time.{LocalDateTime, ZonedDateTime}
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import geotrellis.vector.Polygon
import net.jodah.failsafe.function.CheckedSupplier
import net.jodah.failsafe.{Failsafe, FailsafeExecutor, RetryPolicy}

import javax.ws.rs.client.ClientBuilder
import org.openeo.geotrelliss3.Catalog.buildPolygon

import java.time.temporal.ChronoUnit.SECONDS
import javax.ws.rs.core.Response
import scala.collection.mutable.ListBuffer
import scala.compat.java8.FunctionConverters._

object Catalog {

  def buildPolygon(ulx: Double, uly: Double, brx: Double, bry: Double): Polygon = {
    Polygon((ulx, uly), (brx, uly), (brx, bry), (ulx, bry), (ulx, uly))
  }

  def parseProductIds(response: CatalogResponse): Seq[CatalogEntry] = {
    response.hits.map(hit => CatalogEntry(hit.data.identification.externalId))
  }
}

case class Catalog(mission: String, level: String) {

  def query(startDate: ZonedDateTime, endDate: ZonedDateTime,
            ulx: Double, uly: Double, brx: Double, bry: Double): Seq[CatalogEntry] = {

    query(startDate, endDate, Seq(), buildPolygon(ulx, uly, brx, bry))
  }

  def query(startDate: ZonedDateTime, endDate: ZonedDateTime, tileIds: Seq[String] = Seq(),
            polygon: Polygon = buildPolygon(-180, 90, 180, -90),
            cloudPercentage: Double = 100): Seq[CatalogEntry] = {

    val result = ListBuffer[CatalogEntry]()

    val firstPage = query_page(startDate, endDate, tileIds, polygon, cloudPercentage)

    val totalHits = firstPage.totalHits
    if (totalHits > 10000)
      throw CatalogException("Total hits larger than 10000, which is not supported by paging: either split your job to multiple smaller or implement scroll or search_after.")
    if (totalHits > 0) {
      result ++= Catalog.parseProductIds(firstPage)
      while (result.length < totalHits) {
        val nextPage = query_page(startDate, endDate, tileIds, polygon, cloudPercentage, result.length)
        result ++= Catalog.parseProductIds(nextPage)
      }
    }

    Seq(result: _*)
  }

  def count(startDate: ZonedDateTime, endDate: ZonedDateTime, tileIds: Seq[String] = Seq(),
            ulx: Double = -180, uly: Double = 90, brx: Double = 180, bry: Double = -90, cloudPercentage: Double = 100): Int = {

    query_page(startDate, endDate, tileIds, buildPolygon(ulx, uly, brx, bry), cloudPercentage).totalHits
  }

  private def query_page(startDate: ZonedDateTime, endDate: ZonedDateTime, tileIds: Seq[String],
                         polygon: Polygon, cloudPercentage: Double, fromIndex: Int = 0) = {

    val newEndDate = if (startDate == endDate) startDate.plusDays(1) else endDate

    var target = ClientBuilder.newClient()
      .target("https://sobloo.eu/api/v1/services/search")
      .queryParam("f", s"production.levelCode:eq:$level")
      .queryParam("f", s"acquisition.beginViewingDate:gte:${startDate.toInstant.toEpochMilli}")
      .queryParam("f", s"acquisition.beginViewingDate:lt:${newEndDate.toInstant.toEpochMilli}")
      .queryParam("f", "state.services.download:eq:internal")
      .queryParam("f", s"contentDescription.cloudCoverPercentage:lte:$cloudPercentage")
      .queryParam("from", fromIndex.toString)
      .queryParam("size", "100")
      .queryParam("sort", "acquisition.beginViewingDate")

    if (tileIds.isEmpty) {
      target = target.queryParam("gintersect", polygon.toString)
    } else {
      val tileIdFilter = tileIds.map(t => s"identification.externalId:like:_T${t}_;").mkString("")
      target = target.queryParam("f", tileIdFilter)
    }

    val retryPolicy = {
      val serverError: Response => Boolean = _.getStatus >= 500

      new RetryPolicy[Response]
        .handleResultIf(serverError.asJava)
        .withBackoff(1, 120, SECONDS)
    }

    val failsafe: FailsafeExecutor[Response] = Failsafe
      .`with`(retryPolicy)

    val response = failsafe
      .get(new CheckedSupplier[Response] {
        override def get(): Response = target.request().get()
      })

    if (response.getStatus == 200) {
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)

      mapper.readValue[CatalogResponse](response.readEntity(classOf[String]))
    } else {
      throw CatalogException(s"Status: ${response.getStatus}, Message: ${response.getStatusInfo.getReasonPhrase}")
    }
  }
}

case class CatalogEntry(productId: String) {

  def getS3bucket: String = {
    val timestamp = LocalDateTime.parse(productId.split("_")(2), ofPattern("yyyyMMdd'T'HHmmss"))
    s"dsd-${productId.substring(0, 2).toLowerCase}-${timestamp.getYear}-${timestamp.getMonthValue.formatted("%02d")}"
  }

  def getS3Key: String = {
    val tileId = getTileId
    s"tiles/${tileId.substring(0, 2)}/${tileId(2)}/${tileId.substring(3)}/$productId.SAFE"
  }

  def getTileId: String = {
    productId.split("_")(5).substring(1)
  }
}

case class CatalogException(message: String = "", cause: Throwable = None.orNull) extends Exception(message, cause)

@JsonIgnoreProperties(ignoreUnknown = true)
case class CatalogResponse(@JsonProperty("totalnb") totalHits: Int, collection: String, hits: Seq[Hit])

@JsonIgnoreProperties(ignoreUnknown = true)
case class Hit(md: Md, data: Data)

@JsonIgnoreProperties(ignoreUnknown = true)
case class Md(id: String, timestamp: Long)

@JsonIgnoreProperties(ignoreUnknown = true)
case class Data(identification: Identification)

@JsonIgnoreProperties(ignoreUnknown = true)
case class Identification(profile: String, externalId: String, collection: String, `type`: String)

