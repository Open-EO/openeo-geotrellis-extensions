package org.openeo.geotrellis.geotiff

import java.util
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.render.{ColorMap, DoubleColorMap, IndexedColorMap}

import scala.collection.JavaConverters._

class GTiffOptions extends Serializable {

  var name:Option[String] = None
  var colorMap: Option[ColorMap] = Option.empty
  var tags: Tags = Tags.empty
  var overviews:String = "OFF"
  var resampleMethod:String = "near"

  def setName(name: String): Unit = this.name = Some(name)

  def setColorMap(colors: util.ArrayList[Int]): Unit = {
    colorMap = Some(new IndexedColorMap(colors.asScala))
  }

  def setResampleMethod(method:String): Unit = {
    resampleMethod = method
  }

  /**
   * Remove this hack after updating Scala beyond version 2.12.13
   */
  private def cleanDoubleColorMap(colormap: DoubleColorMap): DoubleColorMap = {
    val mCopy = colormap.breaksString.split(";").map(x => {
      val l = x.split(":");
      // parseUnsignedInt, because there is no minus sign in the hexadecimal representation.
      // When casting an unsigned int to an int, it will correctly overflow
      Tuple2(l(0).toDouble, Integer.parseUnsignedInt(l(1), 16))
    }).toMap
    new DoubleColorMap(mCopy, colormap.options)
  }

  def setColorMap(colors: ColorMap): Unit = {
    colorMap = Some(colors match {
      case c: DoubleColorMap => cleanDoubleColorMap(c)
      case _ => colors
    })
  }

  def addHeadTag(tagName:String,value:String): Unit = {
    tags = Tags(tags.headTags + (tagName -> value), tags.bandTags)
  }

  def addBandTag(bandIndex:Int, tagName:String,value:String): Unit = {
    val emptyMap = Map.empty[String, String]
    var newBandTags = Vector.fill[Map[String,String]](math.max(bandIndex+1,tags.bandTags.size))(emptyMap)
    newBandTags =  newBandTags.zipAll(tags.bandTags,emptyMap,emptyMap).map(elem => elem._1 ++ elem._2)
    newBandTags = newBandTags.updated(bandIndex, newBandTags(bandIndex) + (tagName -> value))
    tags = Tags(tags.headTags ,newBandTags.toList)
  }


}
