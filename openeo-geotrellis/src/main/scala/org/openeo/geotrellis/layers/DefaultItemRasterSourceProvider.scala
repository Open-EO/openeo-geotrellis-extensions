package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.CRS
import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.gdal.{GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.geotiff.{GeoTiffPath, GeoTiffRasterSource, GeoTiffReprojectRasterSource, GeoTiffResampleRasterSource}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, ConvertTargetCellType, GridExtent, RasterExtent, RasterSource, ShortConstantNoDataCellType, TargetAlignment, TargetCellType, TargetRegion, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType}
import geotrellis.vector.ProjectedExtent
import org.openeo.geotrellis.layers.FileLayerProvider.vsis3ToS3
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses
import org.openeo.opensearch.OpenSearchResponses.Feature
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

object DefaultItemRasterSourceProvider extends DefaultItemRasterSourceProvider

case class DefaultItemRasterSourceProvider() extends ItemRasterSourceProvider {

  private val logger: Logger = LoggerFactory.getLogger(classOf[DefaultItemRasterSourceProvider])

  override def canProcess(item: OpenSearchResponses.Feature, datacubeParams: Option[DataCubeParameters] = Option.empty): Boolean = {
    true
  }

  override def getRasterSource(item: OpenSearchResponses.Feature, targetExtent: RasterExtent, targetCRS: CRS, linkTitleToBandIndex: Seq[(String, Int)], datacubeParams: Option[DataCubeParameters], resolver: BandAssetLinkResolver): Option[RasterSource] = {
    logger.debug(s"Getting raster source for item ${item}")
    val maybeTuple = deriveRasterSources(item = item, targetExtent = ProjectedExtent(targetExtent.extent, targetCRS), datacubeParams = datacubeParams, resolver = resolver)
    val maybeSource: Option[RasterSource] = maybeTuple.map(_._1)
    maybeSource
  }

  private def deriveRasterSources(item: Feature, targetExtent: ProjectedExtent, datacubeParams: Option[DataCubeParameters] = Option.empty, targetResolution: Option[CellSize] = Option.empty, resolver: BandAssetLinkResolver): Option[(BandCompositeRasterSource, Feature)] = {
    val noResampleOnRead = datacubeParams.exists(_.noResampleOnRead)
    val theResolution = targetResolution.getOrElse(resolver.maxSpatialResolution)
    val re = RasterExtent(expandToCellSize(targetExtent.extent, theResolution), theResolution)

    val featureExtentInLayout: Option[GridExtent[Long]] = computeItemExtentInTargetLayout(item, re, targetExtent, datacubeParams)

    var predefinedExtent: Option[GridExtent[Long]] = None
    /**
     * Benefit of targetregion: it can be valid in the target projection system
     * Downside of targetregion: it is a virtual cropping of the raster, so we're not able to load data beyond targetExtent
     *
     */
    val alignment =
      if (item.crs.isDefined && item.crs.get.proj4jCrs.getProjection.getName == "utm" && datacubeParams.map(_.maskingStrategyParameters.getOrDefault("method", "")).contains("mask_scl_dilation")) {
        //this hack avoid virtual cropping for Sentinel-2 (utm), which breaks mask_scl_dilation
        TargetAlignment(re)
      } else {
        TargetRegion(re)
      }


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
      def alignmentFromDataPath(dataPath: String, projectedExtent: ProjectedExtent): TargetRegion = {
        // When noResampleOnRead is set, we retrieve the actual resolution from the dataPath.
        // Note: This is only supported for S2 dataPaths.
        // E.g. S2A_20190307T105021_31UFT_TOC-B05_20M_V200.tif = 20.0
        val splitPath: Array[String] = dataPath.split("_")
        val tiffResolution = splitPath(splitPath.length - 2).replace("M", "").toDouble
        val tiffCellSize = CellSize(tiffResolution, tiffResolution)
        val tiffRe = RasterExtent(expandToCellSize(projectedExtent.extent, tiffCellSize), tiffCellSize)
        TargetRegion(tiffRe)
      }
      if (item.crs.isDefined && item.crs.get != null && item.crs.get.equals(targetExtent.crs)) {
        // when we don't know the feature (input) CRS, it seems that we assume it is the same as target extent???
        if (resolver.experimental) {
          GDALRasterSource(dataPath, options = GDALWarpOptions(alignTargetPixels = true, cellSize = Some(theResolution), resampleMethod = Some(resampleMethod)), targetCellType = targetCellType)
        } else {
          val geotiffPath = GeoTiffPath(vsis3ToS3(dataPath))
          if (noResampleOnRead) {
            val tiffAlignment = alignmentFromDataPath(dataPath, targetExtent)
            val geotiffRasterSource = GeoTiffRasterSource(geotiffPath, targetCellType)
            new ResampledRasterSource(geotiffRasterSource, tiffAlignment.region.cellSize, theResolution)
          } else {
            GeoTiffResampleRasterSource(geotiffPath, alignment, resampleMethod, OverviewStrategy.DEFAULT, targetCellType, None)
          }
        }
      } else {
        if (resolver.experimental) {
          val warpOptions = GDALWarpOptions(alignTargetPixels = false, cellSize = Some(theResolution), targetCRS = Some(targetExtent.crs), resampleMethod = Some(resampleMethod), te = Some(targetExtent.extent))
          GDALRasterSource(dataPath.replace("/vsis3/EODATA/", "/vsis3/eodata/").replace("https", "/vsicurl/https"), options = warpOptions, targetCellType = targetCellType)
        } else {
          val geotiffPath = GeoTiffPath(vsis3ToS3(dataPath))
          if (noResampleOnRead) {
            val tiffAlignment = alignmentFromDataPath(dataPath, targetExtent)
            val geotiffRasterSource = GeoTiffReprojectRasterSource(geotiffPath, targetExtent.crs, tiffAlignment, resampleMethod, OverviewStrategy.DEFAULT, targetCellType = targetCellType)
            new ResampledRasterSource(geotiffRasterSource, tiffAlignment.region.cellSize, theResolution)
          } else {
            GeoTiffReprojectRasterSource(geotiffPath, targetExtent.crs, alignment, resampleMethod, OverviewStrategy.DEFAULT, targetCellType = targetCellType)
          }
        }
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
          val path = deriveFilePath(link.href, resolver)
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

          val rasterSourceRaw = rasterSource(path, cloudPath, targetCellType, targetExtent, sentinelXmlAngleBandIndex = bandIndex)
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
          case _ => (NoDataRasterSource.instance(gridExtent, targetExtent.crs), 0)
        }

      val attributes = Predef.Map("date" -> item.nominalDate.toString)

      if (resolver.byLinkTitle && resolver.bandIndices.isEmpty) {
        val actualNumberOfBands = rasterSources.size

        if (actualNumberOfBands != expectedNumberOfBands) {
          logger.warn(s"Did not find expected number of bands $expectedNumberOfBands (actual: $actualNumberOfBands) for feature ${item.id} with links ${item.links.mkString("Array(", ", ", ")")}")
          return None
        }

        Some((new BandCompositeRasterSource(sources.map { case (rasterSource, _) => rasterSource }, targetExtent.crs, attributes, predefinedExtent = predefinedExtent, softErrors = resolver.softErrors), item))
      } else if (sources.forall { case(_, idx) => idx == 0}) {
        Some((new BandCompositeRasterSource(sources.map { case (rasterSource, _) => rasterSource}, targetExtent.crs, attributes, readFullTile = datacubeParams.exists(_.loadPerProduct), predefinedExtent = predefinedExtent), item))
      } else {
        Some((new MultibandCompositeRasterSource(sources.map { case (rasterSource, bandIndex) => (rasterSource, Seq(bandIndex))}, targetExtent.crs, attributes, readFullTile = datacubeParams.exists(_.loadPerProduct), predefinedExtent = predefinedExtent), item))
      }
    }
  }

  private def deriveFilePath(href: URI, resolver: BandAssetLinkResolver): String = href.getScheme match {
    // as oscars requests now use accessedFrom=MEP, we will normally always get file paths
    case "file" => // e.g. file:/data/MTDA_DEV/CGS_S2_DEV/FAPAR_V2/2020/03/19/S2A_20200319T032531_48SXD_FAPAR_V200/10M/S2A_20200319T032531_48SXD_FAPAR_10M_V200.tif
      href.getPath.replaceFirst("CGS_S2_DEV", "CGS_S2") // temporary workaround?
    case "https" if (resolver._rootPath != null) =>
      val hrefString = href.toString
      if (hrefString.contains("artifactory.vgt.vito.be/artifactory/testdata-public")) {
        hrefString
      } else {
        // e.g. https://oscars-dev.vgt.vito.be/download/FAPAR_V2/2020/03/20/S2B_20200320T102639_33VVF_FAPAR_V200/10M/S2B_20200320T102639_33VVF_FAPAR_10M_V200.tif
        val subPath = href.getPath
          .split("/")
          .drop(4) // the empty string at the front too
          .mkString("/")

        (resolver._rootPath resolve subPath).toString
      }
    case _ => href.toString
  }
}
