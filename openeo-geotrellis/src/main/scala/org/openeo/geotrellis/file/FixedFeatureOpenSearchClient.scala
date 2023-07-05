package org.openeo.geotrellis.file

import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.net.URI
import scala.collection.JavaConverters._
import java.time.ZonedDateTime
import java.util

class FixedFeatureOpenSearchClient(id: String, bbox: Extent, nominal_date: String, links: util.List[util.List[String]]) extends OpenSearchClient {
  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] = {
    val nominalDate = ZonedDateTime.parse(nominal_date)

    val sLinks = links.asScala.map { titleHrefPair => Link(href = new URI(titleHrefPair.get(1)), title = Some(titleHrefPair.get(0)))}.toArray
    Seq(Feature(id, this.bbox, nominalDate, sLinks, resolution = None))
  }

  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection = ???

  override def getCollections(correlationId: String): Seq[Feature] = ???
}
