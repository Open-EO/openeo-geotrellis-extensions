package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType}
import geotrellis.vector.Extent
import org.openeo.opensearch.OpenSearchResponses.CreoFeatureCollection

import scala.xml.XML

object SentinelXMLMetadataRasterSource {

  def forAngleBand(xlmPath: String,
                   angleBandIndex: Int,
                   targetExtent: Option[Extent] = None,
                   cellSize: Option[CellSize] = None
                  ): SentinelXMLMetadataRasterSource = {
    // TODO: write forAngleBands in terms of forAngleBand instead
    forAngleBands(xlmPath, Seq(angleBandIndex), targetExtent, cellSize).head
  }

  /**
   * Returns SAA,SZA,VAA,VZA selected by the bands argument.
   */
  def forAngleBands(xlmPath: String,
                    angleBandIndices: Seq[Int] = Seq(0, 1, 2, 3), // SAA, SZA, VAA, VZA
                    te: Option[Extent] = None,
                    cellSize: Option[CellSize] = None
                ): Seq[SentinelXMLMetadataRasterSource] = {
    require(angleBandIndices.forall(index => index >= 0 && index <= 3))

    val theResolution = cellSize.getOrElse(CellSize(10, 10))

    val xmlDoc = XML.load(CreoFeatureCollection.loadMetadata(xlmPath))
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
    val targetExtent = te.getOrElse(maxExtent)
    val gridExtent = GridExtent[Long](targetExtent, theResolution)

    val constantAngleBandValues = Seq(mSAA, mSZA, mVAA, mVZA)

    for {
      index <- angleBandIndices
      angleBandValue = constantAngleBandValues(index)
    } yield new SentinelXMLMetadataRasterSource(angleBandValue, crs, gridExtent, OpenEoSourcePath(xlmPath))
  }
}

class SentinelXMLMetadataRasterSource(value: Float, // TODO: rename this to a more generic constant value RasterSource?
                                      theCrs: CRS,
                                      theGridExtent: GridExtent[Long],
                                      sourcePathName: OpenEoSourcePath,
                                     ) extends RasterSource {

  val targetCellType = None
  val gridSize = 23


  override def metadata: RasterMetadata = ???

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = ???

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = ???

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    Some(Raster(MultibandTile(FloatConstantTile(
      value,
      math.floor(extent.width.intValue() / theGridExtent.cellSize.width).toInt,
      math.floor(extent.height.intValue() / theGridExtent.cellSize.height).toInt,
    )), extent))
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    val extent = gridExtent.extentFor(bounds, clamp = true) // clamp=true gives same results as the intersection don in GDALRasterSource
    Some(Raster(MultibandTile(FloatConstantTile(value, bounds.width.intValue(), bounds.height.intValue())), extent))
  }

  override def convert(targetCellType: TargetCellType): RasterSource = ???

  override def name: SourceName = sourcePathName

  override def crs: CRS = theCrs

  override def bandCount: Int = 1

  override def cellType: CellType = FloatConstantNoDataCellType

  override def gridExtent: GridExtent[Long] = theGridExtent

  override def resolutions: List[CellSize] = List(theGridExtent.cellSize)

  override def attributes: Map[String, String] = Map.empty[String,String]

  override def attributesForBand(band: Int): Map[String, String] = Map.empty[String,String]


}
