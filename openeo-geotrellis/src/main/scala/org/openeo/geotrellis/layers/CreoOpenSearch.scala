package org.openeo.geotrellis.layers

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.OpenSearchResponses._
import scalaj.http.HttpOptions

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT
import scala.collection.Map

object CreoOpenSearch extends OpenSearch {
  private val collections = "https://finder.creodias.eu/resto/collections.json"
  private def collection(collectionId: String) = s"https://finder.creodias.eu/resto/api/collections/$collectionId/search.json"

  override def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                           processingLevel: String, attributeValues: Map[String, Any],
                           correlationId: String): Seq[Feature] = {
    def from(page: Int): Seq[Feature] = {
      val FeatureCollection(itemsPerPage, features) = getProducts(collectionId, start, end, bbox, processingLevel, attributeValues, page, correlationId = correlationId)
      if (itemsPerPage <= 0) Seq() else features ++ from(page + 1)
    }

    from(1)
  }

  override protected def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                                     processingLevel: String, attributeValues: Map[String, Any], page: Int,
                                     correlationId: String): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val getProducts = http(collection(collectionId))
      .param("processingLevel", processingLevel)
      .param("startDate", start format ISO_INSTANT)
      .param("completionDate", end format ISO_INSTANT)
      .param("box", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortParam", "startDate") // paging requires deterministic order
      .param("sortOrder", "ascending")
      .param("page", page.toString)
      .param("maxRecords", "100")
      .param("status", "all")
      .param("dataset", "ESA-DATASET")
      .params(attributeValues.mapValues(_.toString).toSeq)

    val json = withRetries { execute(getProducts) }
    CreoFeatureCollection.parse(json)
  }

  override def getCollections(correlationId: String): Seq[Feature] = {
    val getCollections = http(collections)
      .option(HttpOptions.followRedirects(true))

    val json = withRetries { execute(getCollections) }
    CreoCollections.parse(json).collections.map(c => Feature(c.name, null, null, null, null))
  }
}
