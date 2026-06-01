package org.openeo.geotrellis.layers.provider

import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellSize, GridExtent, RasterExtent, ResampleTarget, TargetAlignment, TargetCellType, TargetRegion}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.net.URI
import java.nio.file.Paths

case class RasterSourceDefinition(link: Link, bandIndex: Int, feature:Feature, rootPath:String, targetCellType:Option[TargetCellType], targetExtent:ProjectedExtent, featureExtentInLayout: Option[GridExtent[Long]], targetResolution: Option[CellSize], maxResolution: CellSize, datacubeParams: Option[DataCubeParameters], experimental: Boolean, bandName: String) {

  lazy val dataPath: String = deriveFilePath(link.href)

  lazy val cloudPath: Option[(String, String)] = for {
    cloudDataPath <- feature.links.find(_.title contains "FineCloudMask_Tile1_Data").map(_.href.toString)
    metadataPath <- feature.links.find(_.title contains "S2_Level-1C_Tile1_Metadata").map(_.href.toString)
  } yield (cloudDataPath, metadataPath)

  val theResolution: CellSize = targetResolution.getOrElse(maxResolution)

  lazy val alignment: ResampleTarget = {
    val re = RasterExtent(expandToCellSize(targetExtent.extent,theResolution), theResolution)
    if(feature.crs.isDefined && feature.crs.get.proj4jCrs.getProjection.getName == "utm" && datacubeParams.map(_.maskingStrategyParameters.getOrDefault("method", "")).contains("mask_scl_dilation")) {
      //this hack avoid virtual cropping for Sentinel-2 (utm), which breaks mask_scl_dilation
      TargetAlignment(re)
    }else{
      TargetRegion(re)
    }
  }

  lazy val resampleMethod: ResampleMethod = datacubeParams.map(_.resampleMethod).getOrElse(NearestNeighbor)


  lazy val noResampleOnRead: Boolean = datacubeParams.exists(_.noResampleOnRead)


  private val _rootPath = if(rootPath != null) Paths.get(rootPath) else null

  private def deriveFilePath(href: URI): String = href.getScheme match {
    // as oscars requests now use accessedFrom=MEP, we will normally always get file paths
    case "file" => // e.g. file:/data/MTDA_DEV/CGS_S2_DEV/FAPAR_V2/2020/03/19/S2A_20200319T032531_48SXD_FAPAR_V200/10M/S2A_20200319T032531_48SXD_FAPAR_10M_V200.tif
      href.getPath.replaceFirst("CGS_S2_DEV", "CGS_S2") // temporary workaround?
    case "https" if( _rootPath !=null ) =>
      val hrefString = href.toString
      if (hrefString.contains("artifactory.vgt.vito.be/artifactory/testdata-public")) {
        hrefString
      } else {
        // e.g. https://oscars-dev.vgt.vito.be/download/FAPAR_V2/2020/03/20/S2B_20200320T102639_33VVF_FAPAR_V200/10M/S2B_20200320T102639_33VVF_FAPAR_10M_V200.tif
        val subPath = href.getPath
          .split("/")
          .drop(4) // the empty string at the front too
          .mkString("/")

        (_rootPath resolve subPath).toString
      }
    case _ => href.toString
  }

private def expandToCellSize(extent: Extent, cellSize: CellSize): Extent =
  Extent(
    extent.xmin,
    extent.ymin,
    math.max(extent.xmax, extent.xmin + cellSize.width),
    math.max(extent.ymax, extent.ymin + cellSize.height),
  )

}
