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

  def addFeature(id: String, bbox: Extent, nominal_date: String, links: util.List[util.List[String]]): Unit = { // href, title, band1, band2, ...
    val nominalDate = ZonedDateTime.parse(nominal_date)
    val sLinks = links.asScala.map { values =>
      val Seq(href, title, bands @ _*) = values.asScala
      Link(href = new URI(href), title = Some(title), bandNames = Some(bands))
    }.toArray

    addFeature(id, bbox, nominalDate, sLinks)
  }

  def addFeature(id: String, bbox: Extent, nominalDate: ZonedDateTime, links: Array[Link]): Unit =
    features += Feature(id, bbox, nominalDate, links, resolution = None)

  override def getProducts(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String): Seq[OpenSearchResponses.Feature] =
    features.toList

  override protected def getProductsFromPage(collectionId: String, dateRange: Option[(ZonedDateTime, ZonedDateTime)], bbox: ProjectedExtent, attributeValues: collection.Map[String, Any], correlationId: String, processingLevel: String, page: Int): OpenSearchResponses.FeatureCollection = ???

  override def getCollections(correlationId: String): Seq[Feature] = ???

  override def equals(other: Any): Boolean = other match {
    case that: FixedFeaturesOpenSearchClient => features == that.features
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(features)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
