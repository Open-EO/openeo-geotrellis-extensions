package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.raster.CellSize
import geotrellis.vector.Extent
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.geotrellis.layers.FileLayerProvider.convertNetcdfLinksToGDALFormat
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}
import org.openeo.opensearch.{OpenSearchClient, OpenSearchResponses}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime
import scala.collection.immutable

case class BandAssetLinkResolver(openSearch: OpenSearchClient, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
                                 maxSpatialResolution: CellSize,
                                 bandIndices: Seq[Int], experimental: Boolean,
                                 maxSoftErrorsRatio: Double) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[BandAssetLinkResolver])

  def mapping(openSearch: OpenSearchClient): Map[(String, String, Extent, ZonedDateTime), Feature] = {
    openSearch match {
      case client: FixedFeaturesOpenSearchClient => {
        val features: Seq[OpenSearchResponses.Feature] = client.getProducts(null, null, null)
        if (features.size > 1
        ) {
          val bandCount = features.flatMap(f => f.links.flatMap(l => l.bandNames.getOrElse(Seq()))).distinct.size
          if (bandCount > 1 && features.map(_.links.flatMap(_.bandNames.getOrElse(Seq()))).forall(_.size != bandCount)) {
            if (features.map(_.collectionId).distinct.size > 1) {
              logger.debug(s"Multiple features with different collectionId found in OpenSearch client, cannot merge into single feature client")
              return immutable.Map.empty
            }
            if (features.map(_.crs).distinct.size > 1) {
              logger.debug(s"Multiple features with different CRS found in OpenSearch client, cannot merge into single feature client")
              return immutable.Map.empty
            }
            if (features.map(_.resolution).distinct.size > 1) {
              logger.debug(s"Multiple features with different resolution found in OpenSearch client, cannot merge into single feature client")
              return immutable.Map.empty
            }
            logger.warn(s"Multiple incomplete features found in OpenSearch client, merging into single feature client with combined links")
            val tupleToFeatures: Map[(ZonedDateTime, Extent), Seq[Feature]] = features.groupBy(f => (f.nominalDate, f.bbox))
            tupleToFeatures.iterator.flatMap { case (_, features) => {
              val links: Array[Link] = features.flatMap(_.links).groupBy(_.bandNames).map(_._2.minBy(_.href)).toArray
              features.map(fe => ((fe.collectionId, fe.id, fe.bbox, fe.nominalDate), OpenSearchResponses.Feature(fe.id, fe.bbox, fe.nominalDate, links, fe.resolution, fe.tileID, fe.geometry, fe.crs, fe.generalProperties, fe.rasterExtent, fe.deduplicationOrderValue, fe.cloudCover, fe.selfUrl)))
            }
            }.toMap
          } else {
            immutable.Map.empty
          }
        } else {
          immutable.Map.empty
        }
      }
      case _ => immutable.Map.empty
    }
  }

  val featureMapping: Map[(String, String, Extent, ZonedDateTime), Feature] = mapping(openSearch)

  val openSearchLinkTitlesWithBandId: Seq[(String, Int)] = {
    openSearch match {
      case client: FixedFeaturesOpenSearchClient =>
        val features: Seq[Feature] = client.asInstanceOf[FixedFeaturesOpenSearchClient].getProducts(null, null, null)
        val bandNameWithIdList: Seq[(String, Int)] = openSearchLinkTitles.map(bandName =>
          (bandName, features.flatMap(_.links).find(_.bandNames.getOrElse(Seq()).contains(bandName)).getOrElse(Link(new URI(""), Some(""), Some(""), Some(Seq()))).bandNames.get.indexOf(bandName))
        ).toList
        bandNameWithIdList
      case _ => {
        if (bandIndices.nonEmpty) {
          //case 1: PROBA-V, geotiff file containing multiple bands, bandids parameter is used to indicate which bands to load
          openSearchLinkTitles.toList zip bandIndices
        } else {
          //case 2: Sentinel-2 angle metadata: band number is encoded in the oscars link title directly, maybe proba could use this system as well...
          openSearchLinkTitles
            .map { title =>
              val Array(t, bandIndex@_*) = title.split("##")
              (t, if (bandIndex.nonEmpty) bandIndex.head.toInt else 0)
            }
            .toList
        }
      }
    }
  }

  val _rootPath: Path = if (rootPath != null) Paths.get(rootPath) else null
  val fromLoadStac: Boolean = openSearch.isInstanceOf[FixedFeaturesOpenSearchClient]
  val byLinkTitle: Boolean = !fromLoadStac
  val softErrors: Boolean = maxSoftErrorsRatio > 0.0
  val bandNames: Seq[String] = openSearchLinkTitles.toList

  def getBandAssets(item: Feature): Seq[Option[(Link, Int, String)]] = {
    if (fromLoadStac) {
      val feature = featureMapping.getOrElse((item.collectionId, item.id, item.bbox, item.nominalDate), item)
      getBandAssetsByBandInfo(feature)
    } else {
      getBandAssetsByLinkTitle(item)
    }
  }

  private def getBandAssetsByBandInfo(item: Feature): Seq[Option[(Link, Int, String)]] = { // [Some((href, bandIndex))]
    def getBandAsset(bandName: String): Option[(Link, Int, String)] = { // (href, bandIndex, bandName)
      val tuples: Array[(Link, Int, String)] = item.links
        .flatMap(link => link.bandNames match {
          case Some(assetBandNames) =>
            val bandIndex = assetBandNames.indexWhere(_ == bandName)
            if (bandIndex >= 0) {
              convertNetcdfLinksToGDALFormat(link, bandName, bandIndex)
            } else None
          case _ => None
        })
      tuples.headOption
        .orElse {
          logger.warn(s"asset with band name $bandName not found in feature ${item.id}; inserting NODATA band instead")
          None
        }
    }

    val maybeTuples = bandNames
      .map(getBandAsset)
    maybeTuples
  }

  private def getBandAssetsByLinkTitle(item: Feature): Seq[Option[(Link, Int, String)]] = for {
    (title, bandIndex) <- openSearchLinkTitlesWithBandId.toList
    linkWithTitle = item.links.find(_.title.map(_.toUpperCase) contains title.toUpperCase).orElse {
      logger.warn(s"asset with ID/title $title not found in feature ${item.id}; inserting NODATA band instead")
      None
    }
  } yield linkWithTitle.map(convertNetcdfLinksToGDALFormat(_, title, bandIndex).get)

}
