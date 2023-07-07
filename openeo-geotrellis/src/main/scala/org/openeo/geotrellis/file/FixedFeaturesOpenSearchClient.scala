package org.openeo.geotrellis.file

import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.net.URI
import scala.collection.JavaConverters._
import java.time.ZonedDateTime
import java.util
import scala.collection.mutable

class FixedFeaturesOpenSearchClient extends OpenSearchClient {
  private val features = mutable.Buffer[Feature]()

  def addFeature(id: String, bbox: Extent, nominal_date: String, links: util.List[util.List[String]]): Unit = {
    val nominalDate = ZonedDateTime.parse(nominal_date)
    val sLinks = links.asScala.map { titleHrefPair => Link(href = new URI(titleHrefPair.get(1)), title = Some(titleHrefPair.get(0))) }.toArray

    features += Feature(id, bbox, nominalDate, sLinks, resolution = None)
  }

  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] =
    features.toList

  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection = ???

  override def getCollections(correlationId: String): Seq[Feature] = ???
}
