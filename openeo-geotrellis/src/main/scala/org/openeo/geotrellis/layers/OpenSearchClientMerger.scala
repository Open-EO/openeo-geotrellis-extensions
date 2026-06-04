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
          if (features.map(_.collectionId).distinct.size > 1) {
            logger.debug(s"Multiple features with different collectionId found in OpenSearch client, cannot merge into single feature client")
            return client
          }
          if (features.map(_.crs).distinct.size > 1) {
            logger.debug(s"Multiple features with different CRS found in OpenSearch client, cannot merge into single feature client")
            return client
          }
          if (features.map(_.resolution).distinct.size > 1) {
            logger.debug(s"Multiple features with different resolution found in OpenSearch client, cannot merge into single feature client")
            return client
          }
          logger.warn(s"Multiple compatible features found in OpenSearch client, merging into single feature client with combined links")
          val mergedFeatureClient = new FixedFeaturesOpenSearchClient()
          features.groupBy(f => (f.nominalDate, f.bbox)).map(f => {
            val f1 = f._2.head
            val links: Array[OpenSearchResponses.Link] = f._2.flatMap(_.links).groupBy(_.title).map(_._2.minBy(_.href)).toArray
            OpenSearchResponses.Feature(f1.id, f1.bbox, f1.nominalDate, links, f1.resolution, f1.tileID, f1.geometry, f1.crs, f1.generalProperties, f1.rasterExtent, f1.deduplicationOrderValue, f1.cloudCover, f1.selfUrl)
          }).foreach(f => mergedFeatureClient.addFeature(f))
          mergedFeatureClient
        } else {
          client
        }
      }
      case _ => openSearch
    }
  }

}
