package org.openeo.geotrellis.layers

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.OverviewStrategy
import geotrellis.raster.{CellSize, CellType, FloatConstantNoDataCellType, FloatConstantTile, GridBounds, GridExtent, MultibandTile, Raster, RasterMetadata, RasterSource, ResampleMethod, ResampleTarget, SourceName, TargetCellType}
import geotrellis.vector.Extent
import org.openeo.opensearch.OpenSearchResponses.CreoFeatureCollection

import java.net.URL
import scala.xml.XML

object SentinelXMLMetadataRasterSource {

  /**
   * Returns SAA,SZA,VAA,VZA selected by the bands argument.
   * @param path
   * @param bands
   * @return
   */
  def apply(path:String, bands:Seq[Int]=Seq(0,1,2,3)): Seq[SentinelXMLMetadataRasterSource] = {
    val xmlDoc = XML.load(CreoFeatureCollection.loadMetadata(path))
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
    // TODO: It appears that the extent should be: (ulx, uly-(10*10980), ulx+(10*10980), uly)
    val extent = Extent(ulx-(10*10980),uly-(10*10980),ulx,uly)
    val gridExtent = GridExtent[Long](extent,CellSize(10,10))
    val allBands = Seq(mSAA,mSZA,mVAA,mVZA)
    bands.map(b => allBands(b)).map(new SentinelXMLMetadataRasterSource(_,crs,gridExtent))

  }
}

class SentinelXMLMetadataRasterSource(value: Float, theCrs:CRS, theGridExtent:GridExtent[Long]) extends RasterSource{

  val targetCellType = None
  val gridSize = 23


  override def metadata: RasterMetadata = ???

  override protected def reprojection(targetCRS: CRS, resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = ???

  override def resample(resampleTarget: ResampleTarget, method: ResampleMethod, strategy: OverviewStrategy): RasterSource = ???

  override def read(extent: Extent, bands: Seq[Int]): Option[Raster[MultibandTile]] = {
    Some(Raster(MultibandTile(FloatConstantTile(value,extent.width.intValue()/10,extent.height.intValue()/10)),extent))
  }

  override def read(bounds: GridBounds[Long], bands: Seq[Int]): Option[Raster[MultibandTile]] = {

    Some(Raster(MultibandTile(FloatConstantTile(value,bounds.width.intValue(),bounds.height.intValue())),null))
  }

  override def convert(targetCellType: TargetCellType): RasterSource = ???

  override def name: SourceName = null

  override def crs: CRS = theCrs

  override def bandCount: Int = 1

  override def cellType: CellType = FloatConstantNoDataCellType

  override def gridExtent: GridExtent[Long] = theGridExtent

  override def resolutions: List[CellSize] = List(CellSize(10,10))

  override def attributes: Map[String, String] = Map.empty[String,String]

  override def attributesForBand(band: Int): Map[String, String] = Map.empty[String,String]


}
