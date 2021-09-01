package org.openeo.geotrellis.geotiff

import java.util

import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.render.{ColorMap, IndexedColorMap}

import scala.collection.JavaConverters._

class GTiffOptions extends Serializable {

  var colorMap: Option[ColorMap] = Option.empty
  var tags: Tags = Tags.empty
  var overviews:String = "OFF"

  def setColorMap(colors: util.ArrayList[Int]): Unit = {
    colorMap = Some(new IndexedColorMap(colors.asScala))
  }

  def setColorMap(colors: ColorMap): Unit = {
    colorMap = Some(colors)
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
