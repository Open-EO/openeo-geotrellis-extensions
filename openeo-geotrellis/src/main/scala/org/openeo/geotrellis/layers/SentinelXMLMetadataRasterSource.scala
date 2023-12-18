package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType}
import geotrellis.vector.{Extent, ProjectedExtent}
import org.openeo.geotrellis.layers.SentinelXMLMetadataRasterSource.logger
import org.openeo.opensearch.OpenSearchResponses.CreoFeatureCollection
import org.slf4j.LoggerFactory

import scala.xml.XML

object SentinelXMLMetadataRasterSource {
  private val logger = LoggerFactory.getLogger(SentinelXMLMetadataRasterSource.getClass)

  def forAngleBand(xlmPath: String,
                   angleBandIndex: Int,
                   targetProjectedExtent: Option[ProjectedExtent] = None,
                   cellSize: Option[CellSize] = None
                  ): SentinelXMLMetadataRasterSource = {
    // TODO: write forAngleBands in terms of forAngleBand instead
    forAngleBands(xlmPath, Seq(angleBandIndex), targetProjectedExtent, cellSize).head
  }

  /**
   * Returns SAA,SZA,VAA,VZA selected by the bands argument.
   */
  def forAngleBands(xlmPath: String,
                    angleBandIndices: Seq[Int] = Seq(0, 1, 2, 3), // SAA, SZA, VAA, VZA
                    targetProjectedExtent: Option[ProjectedExtent] = None,
                    cellSize: Option[CellSize] = None
                   ): Seq[SentinelXMLMetadataRasterSource] = {
    require(angleBandIndices.forall(index => index >= 0 && index <= 3))

    val theResolution = cellSize.getOrElse(CellSize(10, 10))

    val path = CreoFeatureCollection.loadMetadata(xlmPath)
    assert(path != null)
    val xmlDoc = XML.load(path)
    val angles = xmlDoc \\ "Tile_Angles"
    val meanSun = angles \ "Mean_Sun_Angle"
    val mSZA = ( meanSun \ "ZENITH_ANGLE").text.toFloat
    val mSAA = ( meanSun \ "AZIMUTH_ANGLE").text.toFloat
    val meanViewing = angles \ "Mean_Viewing_Incidence_Angle_List" \ "Mean_Viewing_Incidence_Angle" filter (va=>(va \ "@bandId" toString) == "4")
    val mVZA = (meanViewing \ "ZENITH_ANGLE").text.toFloat
    val mVAA = (meanViewing \ "AZIMUTH_ANGLE").text.toFloat

    val geoCoding = xmlDoc \ "Geometric_Info" \ "Tile_Geocoding"
    val crs = CRS.fromName((geoCoding\"HORIZONTAL_CS_CODE").text)
    val position = geoCoding \ "Geoposition"  filter (va=>(va \ "@resolution" toString) == "10")
    val ulx = (position \ "ULX").text.toDouble
    val uly = (position \ "ULY").text.toDouble
    // TODO: Looking at the cloud locations in the accompanying cloud mask file
    val maxExtent = Extent(ulx, uly - (theResolution.width * 10980), ulx + (theResolution.height * 10980), uly)
    val currentPe = ProjectedExtent(maxExtent, crs)
    val projectedExtent = targetProjectedExtent match {
      case None => currentPe
      case Some(pe) =>
        //val currentPeReproject = currentPe.reproject(pe.crs)
        //assert(currentPeReproject.contains(pe.extent)) // This assert fails
        pe
    }
    val gridExtent = GridExtent[Long](projectedExtent.extent, theResolution)

    val constantAngleBandValues = Seq(mSAA, mSZA, mVAA, mVZA)

    for {
      index <- angleBandIndices
      angleBandValue = constantAngleBandValues(index)
    } yield new SentinelXMLMetadataRasterSource(angleBandValue, projectedExtent.crs, gridExtent, OpenEoSourcePath(xlmPath))
  }
}

case class SentinelXMLMetadataRasterSource(
                                            value: Float, // TODO: rename this to a more generic constant value RasterSource?
                                            crs: CRS,
                                            gridExtent: GridExtent[Long],
                                            sourcePathName: OpenEoSourcePath,
                                          ) extends RasterSource {

  val targetCellType: Option[TargetCellType] = None

  override def metadata: RasterMetadata = this

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = {
    val gridExtentNew = resampleTarget(gridExtent.reproject(crs, targetCRS))
    new SentinelXMLMetadataRasterSource(value, targetCRS, gridExtentNew, sourcePathName)
  }

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource =
    reprojection(crs, resampleTarget, method, strategy)

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val supportedBandIndices = Seq(0)
    require(bands == supportedBandIndices)

    extent.intersection(gridExtent.extent)
      .map { intersection =>
        val intersectionGridBounds = gridExtent.gridBoundsFor(intersection).toGridType[Int]
        val tile = FloatConstantTile(value, intersectionGridBounds.width, intersectionGridBounds.height)
        Raster(MultibandTile(tile), intersection)
      }
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val supportedBandIndices = Seq(0)
    require(bands == supportedBandIndices)

    bounds.intersection(gridExtent.dimensions)
      .map { intersection =>
        val intersectionGridBounds = intersection.toGridType[Int]
        val tile = FloatConstantTile(value, intersectionGridBounds.width, intersectionGridBounds.height)
        Raster(MultibandTile(tile), gridExtent.extentFor(intersection))
      }
  }

  override def convert(targetCellType: TargetCellType): RasterSource = {
    if(targetCellType.cellType != FloatConstantNoDataCellType) {
      logger.warn("Ignoring cellType: " + cellType)
    }
    this.copy()
  }

  override def name: SourceName = sourcePathName

  override def bandCount: Int = 1

  override def cellType: CellType = FloatConstantNoDataCellType

  override def resolutions: List[CellSize] = List(gridExtent.cellSize)

  override def attributes: Map[String, String] = Map.empty[String,String]

  override def attributesForBand(band: Int): Map[String, String] = Map.empty[String,String]
}
