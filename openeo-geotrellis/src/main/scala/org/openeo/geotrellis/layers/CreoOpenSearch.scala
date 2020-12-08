package org.openeo.geotrellis.layers

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.OpenSearchResponses._
import scalaj.http.HttpOptions

import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_INSTANT
import java.time.{LocalDate, OffsetTime, ZonedDateTime}
import scala.collection.Map

object CreoOpenSearch {
  val collections = "https://finder.creodias.eu/resto/collections.json"
  val collection = "https://finder.creodias.eu/resto/api/collections/#COLLECTION#/search.json"

  def apply() = new CreoOpenSearch()
}

class CreoOpenSearch extends OpenSearch {
  import CreoOpenSearch._

  def getProducts(collectionId: String, processingLevel: String, from: LocalDate, to: LocalDate, bbox: ProjectedExtent,
                  attributeValues: Map[String, Any] = Map()): Seq[Feature] = {
    val endOfDay = OffsetTime.of(23, 59, 59, 999999999, UTC)

    val start = from.atStartOfDay(UTC)
    val end = to.atTime(endOfDay).toZonedDateTime

    getProducts(collectionId, processingLevel, start, end, bbox, attributeValues)
  }

  def getProducts(collectionId: String, processingLevel: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                  attributeValues: Map[String, Any]): Seq[Feature] = {
    val newAttributeValues = attributeValues + ("processingLevel" -> processingLevel)

    def from(page: Int): Seq[Feature] = {
      val FeatureCollection(itemsPerPage, features) = getProducts(collectionId, start, end, bbox, "", newAttributeValues, page)
      if (itemsPerPage <= 0) Seq() else features ++ from(page + 1)
    }

    from(1)
  }

  def getProducts(collectionId: String, processingLevel: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent, correlationId: String): Seq[Feature] =
    getProducts(collectionId, processingLevel, start, end, bbox, correlationId)

  override protected def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                          correlationId: String, attributeValues: Map[String, Any], page: Int): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val getProducts = http(collection.replace("#COLLECTION#", collectionId))
      .param("startDate", start format ISO_INSTANT)
      .param("completionDate", end format ISO_INSTANT)
      .param("box", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortParam", "startDate") // paging requires deterministic order
      .param("sortOrder", "ascending")
      .param("page", page.toString)
      .param("maxRecords", "10")
      .param("status", "all")
      .param("dataset", "ESA-DATASET")
      .params(attributeValues.mapValues(_.toString).toSeq)

    val json = withRetries { execute(getProducts) }
    CreoFeatureCollection.parse(json)
  }

  def getCollections: Seq[CreoCollection] = {
    val getCollections = http(collections)
      .option(HttpOptions.followRedirects(true))

    val json = withRetries { execute(getCollections) }
    CreoCollections.parse(json).collections
  }
}
