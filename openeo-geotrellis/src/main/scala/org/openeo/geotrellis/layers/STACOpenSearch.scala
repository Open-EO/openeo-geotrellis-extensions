package org.openeo.geotrellis.layers

import java.net.URL
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.ISO_DATE_TIME

import geotrellis.proj4.LatLng
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.OpenSearchResponses.{Feature, FeatureCollection, STACFeatureCollection}
import scalaj.http.HttpOptions

import scala.collection.Map

/**
 *  {'collections': ['sentinel-s2-l1c'], 'query': {'eo:cloud_cover': {'lte': '10'}, 'data_coverage': {'gt': '80'}}}
 * @param endpoint
 */
class STACOpenSearch(private val endpoint: URL=new URL("https://earth-search.aws.element84.com/v0"),
                     private val s3URLS: Boolean = true) extends OpenSearch {
  override def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                           processingLevel: String, attributeValues: Map[String, Any], correlationId: String): Seq[Feature] = {
    def from(startIndex: Int): Seq[Feature] = {
      val FeatureCollection(itemsPerPage, features) = getProducts(collectionId, start, end, bbox, processingLevel, attributeValues, startIndex, correlationId = correlationId)
      if (itemsPerPage <= 0) Seq() else features ++ from(startIndex + 1)
    }

    from(startIndex = 1)
  }

  override protected def getProducts(collectionId: String, start: ZonedDateTime, end: ZonedDateTime, bbox: ProjectedExtent,
                                     processingLevel: String, attributeValues: Map[String, Any], startIndex: Int,
                                     correlationId: String): FeatureCollection = {
    val Extent(xMin, yMin, xMax, yMax) = bbox.reproject(LatLng)

    val getProducts = http(s"$endpoint/search")
      .param("datetime", start.format(ISO_DATE_TIME) + "/" + end.format(ISO_DATE_TIME))
      .param("collections", "[\"" + collectionId + "\"]")
      .param("limit", "100")
      .param("bbox", "[" + (Array(xMin, yMin, xMax, yMax) mkString ",") + "]")
      .param("page", startIndex.toString)


    val json = withRetries {
      execute(getProducts)
    }

    STACFeatureCollection.parse(json,toS3URL = s3URLS)
  }

  override def getCollections(correlationId: String = ""): Seq[Feature] = {
    val getCollections = http(s"$endpoint/collections")
      .option(HttpOptions.followRedirects(true))


    val json = withRetries { execute(getCollections) }
    STACFeatureCollection.parse(json).features
  }

  override def equals(other: Any): Boolean = other match {
    case that: STACOpenSearch => this.endpoint == that.endpoint && this.s3URLS == that.s3URLS
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(endpoint, s3URLS)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
