package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.CRS
import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{ConvertTargetCellType, GridExtent, RasterExtent, RasterSource, ShortConstantNoDataCellType, TargetCellType, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType}
import geotrellis.vector.ProjectedExtent
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.Feature
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

object SentinelJp2RasterSourceProvider extends SentinelJp2RasterSourceProvider

class SentinelJp2RasterSourceProvider extends ItemRasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[SentinelJp2RasterSourceProvider])


  private def deriveFilePath(href: URI): String = href.getScheme match {
    case _ => href.toString
  }


  def canProcess(item: Feature, datacubeParams: Option[DataCubeParameters] = Option.empty): Boolean = {
    item.links.forall(_.href.getPath.endsWith(".jp2"))
  }

  /**
   * Based on a STAC item, create a RasterSource with the selected bands.
   * The RasterSource should be resampled at load time to match the desired resolution and CRS, and should be aligned to the same pixel grid.
   * The extent of the RasterSource does not have to match the targetExtent exactly.
   *
   * @param targetExtent         The RasterExtent to read from the item, coordinates are provided in `targetCRS`
   * @param targetCRS            The required reference system of the output raster source
   * @param linkTitleToBandIndex The bands to return, provided as an item link title and the corresponding band index to load
   * @param item                 The item from which to load the bands.
   * @param datacubeParams       Other parameters that might affect the output.
   * @return
   */
  def getRasterSource(item: Feature, targetExtent: RasterExtent, targetCRS: CRS, linkTitleToBandIndex: Seq[(String, Int)], datacubeParams: Option[DataCubeParameters] = Option.empty, resolver: BandAssetLinkResolver): Option[RasterSource] = {

    logger.debug(s"Getting raster source for item ${item}")
    val theResolution = targetExtent.cellSize
    val re = RasterExtent(expandToCellSize(targetExtent.extent, theResolution), theResolution)

    val featureExtentInLayout: Option[GridExtent[Long]] = computeItemExtentInTargetLayout(item, re, ProjectedExtent(re.extent, targetCRS), datacubeParams)

    var predefinedExtent: Option[GridExtent[Long]] = None


    val resampleMethod = datacubeParams.map(_.resampleMethod).getOrElse(NearestNeighbor)

    def vsisToHttpsCreo(path: String): String = {
      if (path.startsWith("/vsicurl/")) path.replaceFirst("/vsicurl/", "")
      else if (path.startsWith("/vsis3/eodata/"))
        path.replaceFirst("/vsis3/eodata/", "https://finder.creodias.eu/files/")
      else if (path.startsWith("/eodata/"))
        path.replaceFirst("/eodata/", "https://zipper.creodias.eu/get-object?path=/")
      else if (path.startsWith("http")) path
      else {
        logger.warn("unexpected path: " + path)
        path
      }
    }

    def rasterSource(dataPath: String, cloudPath: Option[(String, String)], targetCellType: Option[TargetCellType], targetExtent: ProjectedExtent, sentinelXmlAngleBandIndex: Int): RasterSource = {
      var warpOptionsOvr = Some(OverviewStrategy.DEFAULT)
      if (dataPath.endsWith("SCL_20m.jp2")) {
        // The overviews in the S2 SCL bands can be wrong, so we need to use the original resolution.
        warpOptionsOvr = Some(geotrellis.raster.io.geotiff.Base)
      }
      val alignPixels = !dataPath.contains("NETCDF:") //align target pixels does not yet work with CGLS global netcdfs
      val warpOptions = GDALWarpOptions(alignTargetPixels = alignPixels, cellSize = Some(theResolution), targetCRS = Some(targetExtent.crs), resampleMethod = Some(resampleMethod),
        te = featureExtentInLayout.map(_.extent), teCRS = Some(targetExtent.crs), ovr = warpOptionsOvr
      )
      logger.debug(s"cloudpath: $cloudPath, warp options: $warpOptions")
      if (cloudPath.isDefined) {
        GDALCloudRasterSource(cloudPath.get._1.replace("/vsis3", ""), vsisToHttpsCreo(cloudPath.get._2), GDALPath(dataPath.replace("/vsis3", "")), options = warpOptions, targetCellType = targetCellType)
      } else {
        predefinedExtent = featureExtentInLayout
        GDALRasterSource(GDALPath(dataPath.replace("/vsis3/EODATA/", "/vsis3/eodata/").replace("https", "/vsicurl/https")), options = warpOptions, targetCellType = targetCellType)
      }
    }

    val expectedNumberOfBands = resolver.openSearchLinkTitlesWithBandId.size

    lazy val cloudPath = for {
      cloudDataPath <- item.links.find(_.title contains "FineCloudMask_Tile1_Data").map(_.href.toString)
      metadataPath <- item.links.find(_.title contains "S2_Level-1C_Tile1_Metadata").map(_.href.toString)
    } yield (cloudDataPath, metadataPath)

    val rasterSources: Seq[Option[(RasterSource, Int)]] =
      resolver.getBandAssets(item).map {
        case Some((link, bandIndex)) =>
          val path = deriveFilePath(link.href)
          val pixelValueScale: Double = link.pixelValueScale.getOrElse(1)
          val pixelValueOffset: Double = link.pixelValueOffset.getOrElse(0)

          //special case handling for data that does not declare nodata properly
          val targetCellType = link.title match {
            // An un-used band called "IMG_DATA_Band_SCL_60m_Tile1_Unit" exists, so not specifying the resulution in the if-check.
            case Some(title) if title.contains("SCENECLASSIFICATION_20M") || title.contains("Band_SCL_") => Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0)))
            case Some(title) if title.startsWith("IMG_DATA_") => Some(ConvertTargetCellType(UShortConstantNoDataCellType))
            case Some(title) if resolver.fromLoadStac && title.endsWith("0m") && pixelValueOffset < 0 => Some(ConvertTargetCellType(UShortConstantNoDataCellType)) // TODO: get info from Link object
            case Some(title) if resolver.fromLoadStac && Seq("SCL_20m", "SCL_60m").contains(title) => Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0))) // TODO: get info from Link object
            case _ => None
          }

          val targetTargetCellType: Option[TargetCellType] = link.title match {
            // Sentinel 2 bands can have negative values now.
            case Some(title) if title.contains("SCENECLASSIFICATION_20M") || title.contains("Band_SCL_") => None
            case Some(title) if title.startsWith("IMG_DATA_") => Some(ConvertTargetCellType(ShortConstantNoDataCellType))
            case Some(title) if resolver.fromLoadStac && title.endsWith("0m") && pixelValueOffset < 0 => Some(ConvertTargetCellType(ShortConstantNoDataCellType)) // TODO: get info from Link object
            case _ => None
          }

          val rasterSourceRaw = rasterSource(path, cloudPath, targetCellType, targetExtent = ProjectedExtent(targetExtent.extent, targetCRS), sentinelXmlAngleBandIndex = bandIndex)
          val rasterSourceWrapped = ValueOffsetRasterSource.wrapRasterSource(rasterSourceRaw, pixelValueScale, pixelValueOffset, targetTargetCellType)
          Some((rasterSourceWrapped, bandIndex))
        case _ => None
      }

    if (rasterSources.isEmpty) {
      logger.warn(s"Excluding item ${item.id} with available assets ${item.links.map(_.title).mkString("(", ", ", ")")}")
      None
    } else {
      lazy val gridExtent = predefinedExtent
        .orElse {
          rasterSources.collectFirst {
            case Some((rasterSource, _)) => rasterSource.gridExtent
          }
        }.getOrElse(return None)

      val sources = NonEmptyList.fromListUnsafe(rasterSources.toList)
        .map {
          case Some(rasterSource) => rasterSource
          case _ => (NoDataRasterSource.instance(gridExtent, targetCRS), 0)
        }

      val attributes = Predef.Map("date" -> item.nominalDate.toString)

      if (resolver.byLinkTitle && resolver.bandIndices.isEmpty) {
        val actualNumberOfBands = rasterSources.size

        if (actualNumberOfBands != expectedNumberOfBands) {
          logger.warn(s"Did not find expected number of bands $expectedNumberOfBands (actual: $actualNumberOfBands) for feature ${item.id} with links ${item.links.mkString("Array(", ", ", ")")}")
          return None
        }
        Some(new BandCompositeRasterSource(sources.map { case (rasterSource, _) => rasterSource }, targetCRS, attributes, predefinedExtent = predefinedExtent, softErrors = resolver.softErrors))
      } else if (sources.forall { case (_, idx) => idx == 0 }) {
        Some(new BandCompositeRasterSource(sources.map { case (rasterSource, _) => rasterSource }, targetCRS, attributes, readFullTile = datacubeParams.exists(_.loadPerProduct), predefinedExtent = predefinedExtent))
      } else {
        Some(new MultibandCompositeRasterSource(sources.map { case (rasterSource, bandIndex) => (rasterSource, Seq(bandIndex)) }, targetCRS, attributes, readFullTile = datacubeParams.exists(_.loadPerProduct), predefinedExtent = predefinedExtent))
      }
    }
  }
}
