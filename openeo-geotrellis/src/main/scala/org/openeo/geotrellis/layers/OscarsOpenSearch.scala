package org.openeo.geotrellis.layers

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.OpenSearchResponses.{Feature, FeatureCollection}
import scalaj.http.HttpOptions

import java.net.URL
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_INSTANT
import scala.collection.Map

class OscarsOpenSearch(private val endpoint: URL) extends OpenSearch {
  override def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                           processingLevel: String, attributeValues: Map[String, Any], correlationId: String): Seq[Feature] = {
    def from(startIndex: Int): Seq[Feature] = {
      val FeatureCollection(itemsPerPage, features) = getProducts(collectionId, start, end, bbox, processingLevel, attributeValues, startIndex, correlationId = correlationId)
      if (itemsPerPage <= 0) Seq() else features ++ from(startIndex + itemsPerPage)
    }

    from(startIndex = 1)
  }

  override protected def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                                     processingLevel: String, attributeValues: Map[String, Any], startIndex: Int,
                                     correlationId: String): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val getProducts = http(s"$endpoint/products")
      .param("collection", collectionId)
      .param("start", start format ISO_INSTANT)
      .param("end", end format ISO_INSTANT)
      .param("bbox", Array(xMin, yMin, xMax, yMax) mkString ",")
      .param("sortKeys", "title") // paging requires deterministic order
      .param("startIndex", startIndex.toString)
      .param("accessedFrom", "MEP") // get direct access links instead of download urls
      .params(attributeValues.mapValues(_.toString).toSeq)
      .param("clientId", clientId(correlationId))

    val json = withRetries { execute(getProducts) }
    FeatureCollection.parse(json)
  }

  override def getCollections(correlationId: String = ""): Seq[Feature] = {
    val getCollections = http(s"$endpoint/collections")
      .option(HttpOptions.followRedirects(true))
      .param("clientId", clientId(correlationId))

    val json = withRetries { execute(getCollections) }
    FeatureCollection.parse(json).features
  }

  override def equals(other: Any): Boolean = other match {
    case that: OscarsOpenSearch => this.endpoint == that.endpoint
    case _ => false
  }

  override def hashCode(): Int = endpoint.hashCode()
}
