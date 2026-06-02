package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.raster.CellSize
import org.openeo.geotrellis.file.FixedFeaturesOpenSearchClient
import org.openeo.geotrellis.layers.FileLayerProvider.convertNetcdfLinksToGDALFormat
import org.openeo.opensearch.OpenSearchClient
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI
import java.nio.file.{Path, Paths}

case class BandAssetLinkResolver(openSearch: OpenSearchClient, openSearchLinkTitles: NonEmptyList[String], rootPath: String,
                                 maxSpatialResolution: CellSize,
                                 bandIndices: Seq[Int], experimental: Boolean,
                                 maxSoftErrorsRatio: Double) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[BandAssetLinkResolver])

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

  def getBandAssets(item: Feature): Seq[Option[(Link, Int)]] = {
    if (fromLoadStac) {
      getBandAssetsByBandInfo(item)
    } else {
      getBandAssetsByLinkTitle(item)
    }
  }

  private def getBandAssetsByBandInfo(item: Feature): Seq[Option[(Link, Int)]] = { // [Some((href, bandIndex))]
    def getBandAsset(bandName: String): Option[(Link, Int)] = { // (href, bandIndex)
      item.links
        .flatMap(link => link.bandNames match {
          case Some(assetBandNames) =>
            val bandIndex = assetBandNames.indexWhere(_ == bandName)
            if (bandIndex >= 0) {
              convertNetcdfLinksToGDALFormat(link, bandName, bandIndex)
            } else None
          case _ => None
        })
        .headOption
        .orElse {
          logger.warn(s"asset with band name $bandName not found in feature ${item.id}; inserting NODATA band instead")
          None
        }
    }

    bandNames
      .map(getBandAsset)
  }

  private def getBandAssetsByLinkTitle(item: Feature): Seq[Option[(Link, Int)]] = for {
    (title, bandIndex) <- openSearchLinkTitlesWithBandId.toList
    linkWithTitle = item.links.find(_.title.map(_.toUpperCase) contains title.toUpperCase).orElse {
      logger.warn(s"asset with ID/title $title not found in feature ${item.id}; inserting NODATA band instead")
      None
    }
  } yield linkWithTitle.map(convertNetcdfLinksToGDALFormat(_, title, bandIndex).get)

}
