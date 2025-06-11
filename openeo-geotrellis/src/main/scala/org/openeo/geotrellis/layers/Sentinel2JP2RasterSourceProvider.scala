package org.openeo.geotrellis.layers

import cats.data.NonEmptyList
import geotrellis.proj4.CRS
import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.gdal.{GDALPath, GDALRasterSource, GDALWarpOptions}
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, ConvertTargetCellType, GridExtent, RasterExtent, RasterSource, ShortConstantNoDataCellType, TargetAlignment, TargetCellType, TargetRegion, UByteUserDefinedNoDataCellType, UShortConstantNoDataCellType}
import geotrellis.vector.ProjectedExtent
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

class Sentinel2JP2RasterSourceProvider {

  private implicit val logger: Logger = LoggerFactory.getLogger(classOf[Sentinel2JP2RasterSourceProvider])


  private def deriveFilePath(href: URI): String = href.getScheme match {

    case _ => href.toString
  }


  def canProcess(item: Feature): Boolean = {
    //check if contains sentinel-2 jp2 files,
    return true
  }

  /**
   * Based on a STAC item, create a RasterSource with the selected bands.
   * The RasterSource should be resampled at load time to match the desired resolution and CRS, and should be aligned to the same pixel grid.
   * The extent of the RasterSource does not have to match the targetExtent exactly.
   *
   *
   * @param targetExtent The RasterExtent to read from the item, coordinates are provided in `targetCRS`
   * @param targetCRS The required reference system of the output raster source
   * @param linkTitleToBandIndex The bands to return, provided as an item link title and the corresponding band index to load
   * @param item The item from which to load the bands.
   * @param datacubeParams Other parameters that might affect the output.
   * @return
   */
  def getRasterSource(targetExtent: RasterExtent, targetCRS:CRS, linkTitleToBandIndex: Seq[(String, Int)], item: Feature, datacubeParams : Option[DataCubeParameters] = Option.empty): Option[RasterSource] = {

    val theResolution = targetExtent.cellSize
    val re = RasterExtent(FileLayerProvider.expandToCellSize(targetExtent.extent,theResolution), theResolution)

    val featureExtentInLayout: Option[GridExtent[Long]] = FileLayerProvider.computeItemExtentInTargetLayout(item, re, ProjectedExtent(re.extent,targetCRS), datacubeParams)

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

    def rasterSource(dataPath:String, cloudPath:Option[(String,String)], targetCellType:Option[TargetCellType],  sentinelXmlAngleBandIndex: Int): RasterSource = {
      if(dataPath.endsWith(".jp2") || dataPath.contains("NETCDF:")) {
        var warpOptionsOvr = Some(OverviewStrategy.DEFAULT)
        if (dataPath.endsWith("SCL_20m.jp2")) {
          // The overviews in the S2 SCL bands can be wrong, so we need to use the original resolution.
          warpOptionsOvr = Some(geotrellis.raster.io.geotiff.Base)
        }
        val warpOptions = GDALWarpOptions(alignTargetPixels = true, cellSize = Some(theResolution), targetCRS = Some(targetCRS), resampleMethod = Some(resampleMethod),
          te = featureExtentInLayout.map(_.extent), teCRS = Some(targetCRS), ovr = warpOptionsOvr
        )
        if (cloudPath.isDefined) {
          GDALCloudRasterSource(cloudPath.get._1.replace("/vsis3", ""), vsisToHttpsCreo(cloudPath.get._2), GDALPath(dataPath.replace("/vsis3", "")), options = warpOptions, targetCellType = targetCellType)
        } else {
          predefinedExtent = featureExtentInLayout
          GDALRasterSource(GDALPath(dataPath.replace("/vsis3/eodata/", "/vsis3/EODATA/").replace("https", "/vsicurl/https")), options = warpOptions, targetCellType = targetCellType)
        }
      }else if(dataPath.endsWith("MTD_TL.xml")) {
        val targetProjectedExtent = featureExtentInLayout match {
          case None => None
          case Some(featureExtentInLayoutGet) =>
            Some(ProjectedExtent(featureExtentInLayoutGet.extent, targetCRS))
        }
        SentinelXMLMetadataRasterSource.forAngleBand(dataPath, sentinelXmlAngleBandIndex, targetProjectedExtent, Some(theResolution))
      }else{
        null
      }
    }


    def getBandAssetsByLinkTitle : Seq[Option[(Link, Int)]] = for {
      (title, bandIndex) <- linkTitleToBandIndex.toList
      linkWithTitle = item.links.find(_.title.map(_.toUpperCase) contains title.toUpperCase).orElse {
        logger.warn(s"asset with ID/title $title not found in feature ${item.id}; inserting NODATA band instead")
        None
      }
    } yield linkWithTitle.map((_,bandIndex))


    val expectedNumberOfBands = linkTitleToBandIndex.size

    lazy val cloudPath = for {
      cloudDataPath <- item.links.find(_.title contains "FineCloudMask_Tile1_Data").map(_.href.toString)
      metadataPath <- item.links.find(_.title contains "S2_Level-1C_Tile1_Metadata").map(_.href.toString)
    } yield (cloudDataPath, metadataPath)

    val rasterSources: Seq[Option[(RasterSource, Int)]] =
      (getBandAssetsByLinkTitle).map {
        case Some((link, bandIndex)) =>
          val path = deriveFilePath(link.href)
          val pixelValueOffset: Double = link.pixelValueOffset.getOrElse(0)

          //special case handling for data that does not declare nodata properly
          val targetCellType = link.title match {
            // An un-used band called "IMG_DATA_Band_SCL_60m_Tile1_Unit" exists, so not specifying the resulution in the if-check.
            case Some(title) if title.contains("SCENECLASSIFICATION_20M") || title.contains("Band_SCL_") => Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0)))
            case Some(title) if title.startsWith("IMG_DATA_") => Some(ConvertTargetCellType(UShortConstantNoDataCellType))
            case Some(title) if title.endsWith("0m") && pixelValueOffset < 0 => Some(ConvertTargetCellType(UShortConstantNoDataCellType)) // TODO: get info from Link object
            case Some(title) if Seq("SCL_20m", "SCL_60m").contains(title) => Some(ConvertTargetCellType(UByteUserDefinedNoDataCellType(0))) // TODO: get info from Link object
            case _ => None
          }

          val targetTargetCellType: Option[TargetCellType] = link.title match {
            // Sentinel 2 bands can have negative values now.
            case Some(title) if title.contains("SCENECLASSIFICATION_20M") || title.contains("Band_SCL_") => None
            case Some(title) if title.startsWith("IMG_DATA_") => Some(ConvertTargetCellType(ShortConstantNoDataCellType))
            case Some(title) if title.endsWith("0m") && pixelValueOffset < 0 => Some(ConvertTargetCellType(ShortConstantNoDataCellType)) // TODO: get info from Link object
            case _ => None
          }

          val rasterSourceRaw = rasterSource(path, cloudPath, targetCellType, sentinelXmlAngleBandIndex = bandIndex)
          val rasterSourceWrapped = ValueOffsetRasterSource.wrapRasterSource(rasterSourceRaw, pixelValueOffset, targetTargetCellType)
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
      val actualNumberOfBands = rasterSources.size

      if (actualNumberOfBands != expectedNumberOfBands) {
        logger.warn(s"Did not find expected number of bands $expectedNumberOfBands (actual: $actualNumberOfBands) for feature ${item.id} with links ${item.links.mkString("Array(", ", ", ")")}")
        return None
      }

      Some(new BandCompositeRasterSource(sources.map { case (rasterSource, _) => rasterSource }, targetCRS, attributes, predefinedExtent = predefinedExtent, softErrors = true))

    }
  }

}
