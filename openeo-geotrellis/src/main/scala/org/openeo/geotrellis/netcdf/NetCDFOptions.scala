package org.openeo.geotrellis.netcdf

import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.render.{ColorMap, IndexedColorMap}
import org.openeo.geotrellis.FormatOptions

import java.util
import java.util.ArrayList
import scala.collection.JavaConverters._

class NetCDFOptions extends FormatOptions {


  var bandNames: Option[util.ArrayList[String]] = None
  var dimensionNames: Option[java.util.Map[String,String]] = None
  var attributes: Option[java.util.Map[String,String]] = None
  var zLevel:Int = 6
  var bandsMetadata: Option[java.util.Map[String,java.util.Map[String,String]]] = None

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

  def setBandsMetadata(metadata: java.util.Map[String,java.util.Map[String,String]]): Unit = {
    bandsMetadata = Option(metadata)
  }


}
