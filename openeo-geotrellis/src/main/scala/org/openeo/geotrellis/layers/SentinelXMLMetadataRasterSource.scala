package org.openeo.geotrellis.layers

import java.net.URL

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType}
import geotrellis.vector.Extent

import scala.xml.XML

class SentinelXMLMetadataRasterSource(path:URL) extends RasterSource{

  val targetCellType = None
  val gridSize = 23

  // viewing zenith angle [band][sensor][row-col]
  var VZA: Array[Array[Array[Float]]] = Array.ofDim[Array[Float]](20,20)
  // viewing azimuth angle [band][sensor][row-col]
  var VAA: Array[Array[Array[Float]]] = Array.ofDim[Array[Float]](20,20)
  // sun zenith angle [row-col]
  var SZA: Array[Float] = null
  // sun azimuth angle [row-col]
  var SAA: Array[Float] = null
  // sun-viewing elative azimuth angle [row-col]
  var RAA: Array[Float] = null

  // mean versions
  var mVZA = .0
  var mVAA = .0
  var mSZA = .0
  var mSAA = .0
  var mRAA = .0

  var _crs:CRS = null

  read_xml(path)

  override def metadata: RasterMetadata = ???

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = ???

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = ???

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    Some(Raster(MultibandTile(FloatConstantTile(mSAA.toFloat,extent.width.intValue()/10,extent.height.intValue()/10)),extent))
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = ???

  override def convert(targetCellType: TargetCellType): RasterSource = ???

  override def name: SourceName = path.toString

  override def crs: CRS = _crs

  override def bandCount: Int = 4

  override def cellType: CellType = FloatConstantNoDataCellType

  override def gridExtent: GridExtent[Long] = ???

  override def resolutions: List[CellSize] = ???

  override def attributes: Map[String, String] = ???

  override def attributesForBand(band: Int): Map[String, String] = ???


  def read_xml(file: URL): Unit = { // read xml
    val xmlDoc = XML.load(file)
    val angles = xmlDoc \\ "Tile_Angles"
    val meanSun = angles \ "Mean_Sun_Angle"
    mSZA = ( meanSun \ "ZENITH_ANGLE").text.toFloat
    mSAA = ( meanSun \ "AZIMUTH_ANGLE").text.toFloat
    val meanViewing = angles \"Mean_Viewing_Incidence_Angle_List"\ "Mean_Viewing_Incidence_Angle" filter (va=>(va \ "@bandId" toString) == "4")
    mVZA = (meanViewing \ "ZENITH_ANGLE").text.toFloat
    mVAA = (meanViewing \ "AZIMUTH_ANGLE").text.toFloat

    _crs = CRS.fromName((xmlDoc \"Geometric_Info"\"Tile_Geocoding"\"HORIZONTAL_CS_CODE").text)
  }


}
