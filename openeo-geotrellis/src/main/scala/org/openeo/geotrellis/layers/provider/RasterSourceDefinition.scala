package org.openeo.geotrellis.layers.provider

import geotrellis.raster.ResampleMethods.NearestNeighbor
import geotrellis.raster.resample.ResampleMethod
import geotrellis.raster.{CellSize, GridExtent, RasterExtent, ResampleTarget, TargetAlignment, TargetCellType, TargetRegion}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrelliscommon.DataCubeParameters
import org.openeo.opensearch.OpenSearchResponses.{Feature, Link}

import java.net.URI
import java.nio.file.Paths

case class RasterSourceDefinition(link: Link, bandIndex: Int, feature:Feature, rootPath:String, targetCellType:Option[TargetCellType], targetExtent:ProjectedExtent, featureExtentInLayout: Option[GridExtent[Long]], targetResolution: Option[CellSize], maxResolution: CellSize, datacubeParams: Option[DataCubeParameters], experimental: Boolean) {

  lazy val dataPath: String = {
    val dataPathArg = deriveFilePath(link.href)

    import java.nio.file.Files
    import java.nio.file.Paths
    val dataPath1 = if (dataPathArg.startsWith("NETCDF:/") && dataPathArg.contains(":")) {
      // ex: NETCDF:/data/MTDA/BIOPAR/BioPar_LAI300_V1_Global/2017/20170110_3/c_gls_LAI300_201701100000_GLOBE_PROBAV_V1.0.1/c_gls_LAI300_201701100000_GLOBE_PROBAV_V1.0.1.nc:LAI
      dataPathArg.replace("NETCDF:/", "/").split(":").head
    } else {
      dataPathArg
    }
    val dataPath2 = if (dataPath1.toLowerCase.startsWith("/data/")) {
      val dataPathCopy = dataPath1.replace("/data/", "/dataCOPY/")
      if (Files.exists(Paths.get(dataPathCopy))) {
        dataPathCopy
      } else if (Files.exists(Paths.get("/dataCOPY/"))
        && Files.exists(Paths.get(dataPath1))) { // Don't attempt to copy "/bogus"
        println("COPY dataPath: " + dataPathCopy)
        Files.createDirectories(Paths.get(dataPathCopy).getParent)
        val tmpBeforeAtomicMove = Paths.get(dataPathCopy + "_unconfirmed_download_" + java.util.UUID.randomUUID())
        Files.copy(Paths.get(dataPath1), tmpBeforeAtomicMove)
        Files.move(tmpBeforeAtomicMove, Paths.get(dataPathCopy))
        dataPathCopy
      } else {
        dataPath1
      }
    } else {
      dataPath1
    }

    val dataPath3 = dataPath2.replace("/vsis3/", "/").replace("s3://eodata/", "/eodata/")
    val dataPath4 = if (dataPath3.toLowerCase.startsWith("/eodata/")) {
      val eodataPathCopy = "/eodata_CACHE/eodata/" + dataPath3.substring("/eodata/".length)
      if (Files.exists(Paths.get(eodataPathCopy))) {
        eodataPathCopy
      } else if (Files.exists(Paths.get(dataPath3))) {
        dataPath3
      } else if (Files.exists(Paths.get("/eodata_CACHE/eodata/"))) {
        // fallback for nested buckets, or if S3 not mounted
        val cmd = s"""s3cmd -c /home/emile/openeo/VITO/VITO2024/.s3cfg_dataspace_copernicus -r get s3://${dataPath3.replace("/eodata/", "EODATA/")} ${eodataPathCopy}""";
        println("CMD: " + cmd)

        sys.process.Process(cmd).!
        eodataPathCopy
      } else if (Files.exists(Paths.get("/eodata_CACHE/eodata/"))
        && Files.exists(Paths.get(dataPath3))) { // Don't attempt to copy "/bogus"
        println("COPY dataPath: " + eodataPathCopy)
        Files.createDirectories(Paths.get(eodataPathCopy).getParent)
        val tmpBeforeAtomicMove = Paths.get(eodataPathCopy + "_unconfirmed_download_" + java.util.UUID.randomUUID())
        Files.copy(Paths.get(dataPath3), tmpBeforeAtomicMove)
        Files.move(tmpBeforeAtomicMove, Paths.get(eodataPathCopy))
        eodataPathCopy
      } else {
        dataPath2
      }
    } else if (dataPath3.startsWith("s3://HRVPP")) {
      dataPath3.replace("s3://HRVPP", "/HRVPP")
    } else {
      dataPath2
    }

    val dataPath = if (dataPathArg.startsWith("NETCDF:/")) {
      "NETCDF:" + dataPath4 + ":" + dataPathArg.split(":").last
    } else {
      dataPath4
    }
    dataPath
  }

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
