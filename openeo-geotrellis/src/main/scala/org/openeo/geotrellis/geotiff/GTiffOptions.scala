package org.openeo.geotrellis.geotiff

import java.util
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.render.{ColorMap, DoubleColorMap, IndexedColorMap}

import scala.collection.JavaConverters._

class GTiffOptions extends Serializable {

  var filenamePrefix = "openEO" // Example using default prefix: "openEO_2017-01-02Z.tif"
  var colorMap: Option[ColorMap] = Option.empty
  var tags: Tags = Tags.empty
  var overviews:String = "OFF"
  var resampleMethod:String = "near"
  var separateAssetPerBand = false

  private val xmlTags = collection.mutable.Buffer[String]() // TODO: improve

  def setFilenamePrefix(name: String): Unit = this.filenamePrefix = name

  def setSeparateAssetPerBand(value: Boolean): Unit = this.separateAssetPerBand = value

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

  def addHeadTag(tagName:String, value:String): Unit = {
    tags = Tags(tags.headTags + (tagName -> value), tags.bandTags)

    xmlTags append asItemElement(tagName, value)
  }

  def addBandTag(bandIndex: Int, tagName:String, value:String, role: Option[String]): Unit = {
    val emptyMap = Map.empty[String, String]
    var newBandTags = Vector.fill[Map[String,String]](math.max(bandIndex+1,tags.bandTags.size))(emptyMap)
    newBandTags =  newBandTags.zipAll(tags.bandTags,emptyMap,emptyMap).map(elem => elem._1 ++ elem._2)
    newBandTags = newBandTags.updated(bandIndex, newBandTags(bandIndex) + (tagName -> value))
    tags = Tags(tags.headTags ,newBandTags.toList)

    xmlTags append asItemElement(tagName, value, Some(bandIndex), role)
  }

  def addBandTag(bandIndex: Int, tagName: String, value: String): Unit =
    addBandTag(bandIndex, tagName, value, role = None)

  def setBandTags(newBandTags: List[Map[String, String]]): Unit = {
    tags = Tags(tags.headTags, newBandTags)

    xmlTags.clear()
    for ((tagName, value) <- tags.headTags) addHeadTag(tagName, value)
    for {
      (band, bandIndex) <- newBandTags.zipWithIndex
      (tagName, value) <- band
    } addBandTag(bandIndex, tagName, value)
  }

  def toGdalMetadataXml: String = {
    val buffer = new StringBuilder("<GDALMetadata>")
    for (xmlTag <- xmlTags) buffer.appendAll(xmlTag)
    buffer.appendAll("</GDALMetadata>")

    buffer.toString
  }

  /**
   * Avoids error when using .clone():
   * "method clone in class Object cannot be accessed in org.openeo.geotrellis.geotiff.GTiffOptions"
   */
  def deepClone(): GTiffOptions = {
    // https://www.avajava.com/tutorials/lessons/how-do-i-perform-a-deep-clone-using-serializable.html
    // TODO: Check for a better implementation
    val baos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(baos)
    oos.writeObject(this)
    oos.close()
    val bais = new java.io.ByteArrayInputStream(baos.toByteArray())
    val ois = new java.io.ObjectInputStream(bais)
    ois.readObject().asInstanceOf[GTiffOptions]
  }

  private def asItemElement(name: String, value: Any, index: Option[Int] = None,
                            role: Option[String] = None): String = {
    val sampleAttribute = index.map(i => s"""sample="$i"""").getOrElse("")
    val roleAttribute = role.map(r => s"""role="$r"""").getOrElse("")
    s"""<Item name="$name" $sampleAttribute $roleAttribute>$value</Item>"""
  }
}
