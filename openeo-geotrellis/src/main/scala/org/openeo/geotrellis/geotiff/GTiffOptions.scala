package org.openeo.geotrellis.geotiff

import java.util
import geotrellis.raster.io.geotiff.Tags
import geotrellis.raster.render.{ColorMap, DoubleColorMap, IndexedColorMap}

import scala.collection.JavaConverters._

//noinspection ScalaUnusedSymbol
class GTiffOptions extends Serializable {

  var filenamePrefix = "openEO" // Example using default prefix: "openEO_2017-01-02Z.tif"
  var colorMap: Option[ColorMap] = Option.empty
  var filepathPerBand: Option[util.ArrayList[String]] = Option.empty
  var tags: Tags = Tags.empty
  var overviews:String = "OFF"
  var resampleMethod:String = "near"
  var separateAssetPerBand = false

  def setFilenamePrefix(name: String): Unit = {
    assertSafeToUseInFilePath(name)
    this.filenamePrefix = name
  }

  def setSeparateAssetPerBand(value: Boolean): Unit = this.separateAssetPerBand = value

  def setFilepathPerBand(value: Option[util.ArrayList[String]]): Unit = {
    if (value.isDefined) {
      // Check in lower case because Windows does not make the distinction
      val valueLowercase = value.get.asScala.map(_.toLowerCase.replace("<date>", "")).toList
      valueLowercase.foreach(filepath => {
        assertSafeToUseInFilePath(filepath)

        if (!filepath.endsWith(".tiff") && !filepath.endsWith(".tif")) {
          throw new IllegalArgumentException("File name must end with .tiff or .tif: " + filepath)
        }
      })

      if (valueLowercase.size != valueLowercase.distinct.size) {
        throw new IllegalArgumentException("All paths in 'filepath_per_band' must be unique: " + value)
      }
    }
    this.filepathPerBand = value
  }

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

  def setBandTags(newBandTags: List[Map[String, String]]): Unit = {
    tags = Tags(tags.headTags, newBandTags)
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

  def assertNoConflicts(): Unit = {
    if (filepathPerBand.isDefined) {
      if (!separateAssetPerBand) {
        throw new IllegalArgumentException("filepath_per_band is only supported with separate_asset_per_band.")
      }
      if (filenamePrefix != "openEO") { // Compare with default value
        throw new IllegalArgumentException("filepath_per_band is not supported with filename_prefix.")
      }
      if (tags.bandCount != filepathPerBand.get.size()) {
        throw new IllegalArgumentException("filepath_per_band size does not match the number of bands. " +
          s"${tags.bandCount} != ${filepathPerBand.get.size()}.")
      }
    }
  }
}
