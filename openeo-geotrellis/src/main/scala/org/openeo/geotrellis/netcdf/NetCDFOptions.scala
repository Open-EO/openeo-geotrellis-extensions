package org.openeo.geotrellis.netcdf

import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.render.{ColorMap, IndexedColorMap}
import geotrellis.vector.Extent

import java.util
import java.util.ArrayList
import scala.collection.JavaConverters._

class NetCDFOptions extends Serializable {


  var bandNames: Option[util.ArrayList[String]] = None
  var dimensionNames: Option[java.util.Map[String,String]] = None
  var attributes: Option[java.util.Map[String,String]] = None
  var zLevel:Int = 6
  var cropBounds:Option[Extent]=Option.empty[Extent]

  def setBandNames(names: util.ArrayList[String]): Unit = {
    bandNames = Option(names)
  }

  def setDimensionNames(names: java.util.Map[String,String]): Unit = {
    dimensionNames = Option(names)
  }

  def setAttributes(attributes: java.util.Map[String,String]):Unit = {
    this.attributes = Option(attributes)
  }

  def setZLevel(level:Int):Unit = {
    zLevel = level
  }

  def setCropBounds(extent:Extent): Unit = {
    cropBounds = Option(extent)
  }


}
