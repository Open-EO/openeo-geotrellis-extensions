package org.openeo.geotrellis.zarr
import java.util
import scala.jdk.CollectionConverters.asScalaBufferConverter

class ZarrOptions  extends Serializable {
  var bandNames = Array.fill(1)("Undefined")
  var numberBands = 1

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
