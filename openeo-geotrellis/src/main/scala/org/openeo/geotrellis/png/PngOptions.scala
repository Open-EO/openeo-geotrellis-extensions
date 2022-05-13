package org.openeo.geotrellis.png

import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.render.{ColorMap, IndexedColorMap}

import java.util
import scala.collection.JavaConverters._

class PngOptions extends Serializable {

  var colorMap: Option[ColorMap] = Option.empty

  def setColorMap(colors: util.ArrayList[Int]): Unit = {
    colorMap = Some(new IndexedColorMap(colors.asScala))
  }

  def setColorMap(colors: ColorMap): Unit = {
    colorMap = Some(colors)
  }



}
