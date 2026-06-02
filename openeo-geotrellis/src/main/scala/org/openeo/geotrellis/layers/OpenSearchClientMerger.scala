package org.openeo.geotrellis.layers

import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.slf4j.{Logger, LoggerFactory}

object OpenSearchClientMerger {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[FileLayerProvider])

  def merge(openSearch: OpenSearchClient): OpenSearchClient = {
    openSearch match {
      case client: FixedFeaturesOpenSearchClient => {
        val features: Seq[OpenSearchResponses.Feature] = client.getProducts(null, null, null)
        if (features.size > 1
        ) {
          if (features.map(_.crs).distinct.size > 1) {
            logger.warn(s"Multiple features with different CRS found in OpenSearch client, cannot merge into single feature client")
            return client
          }
          if (features.map(_.resolution).distinct.size > 1) {
            logger.warn(s"Multiple features with different resolution found in OpenSearch client, cannot merge into single feature client")
            return client
          }
          if (features.map(_.bbox).distinct.size > 1) {
            logger.warn(s"Multiple features with different bbox found in OpenSearch client, cannot merge into single feature client")
            return client
          }
          if (features.map(_.geometry).distinct.size > 1) {
            logger.warn(s"Multiple features with different geometry found in OpenSearch client, cannot merge into single feature client")
            return client
          }
          logger.warn(s"Multiple features found in OpenSearch client, merging into single feature client with combined links")
          val singleFeatureClient = new FixedFeaturesOpenSearchClient()
          features.groupBy(_.nominalDate).map(dateFeature => {
            val f1 = dateFeature._2.head
            val links: Array[OpenSearchResponses.Link] = dateFeature._2.flatMap(_.links).toArray
            OpenSearchResponses.Feature("merged", f1.bbox, f1.nominalDate, links, f1.resolution, f1.tileID, f1.geometry, f1.crs, f1.generalProperties, f1.rasterExtent, f1.deduplicationOrderValue, f1.cloudCover, f1.selfUrl)
          }).foreach(f => singleFeatureClient.addFeature(f))
          singleFeatureClient
        } else {
          client
        }
      }
      case _ => openSearch
    }
  }

}
