package org.openeo.geotrellis.zarr
import geotrellis.vector.Extent
import java.util
import scala.jdk.CollectionConverters._

class ZarrOptions  extends Serializable {
  var bandNames = Array.fill(1)("Undefined")
  var numberBands = 1
  var cropBounds: Option[Extent] = Option.empty[Extent]

  def setCropBounds(extent: Extent): Unit = {
    cropBounds = Option(extent)
  }

  def setBands(nBands:Int, names:util.ArrayList[String]):Unit = {
    this.numberBands = nBands
    this.bandNames =
      if (names.size()==nBands) {
        names.asScala.toArray[String]
      } else {
        Array.fill(nBands)("Undefined")
      }
  }

  def setBands(nBands:Int): Unit = {
    this.numberBands = nBands
    this.bandNames = Array.fill(nBands)("Undefined")
  }

}
