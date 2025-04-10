package org.openeo.geotrellis.zarr
import java.util
import scala.jdk.CollectionConverters.asScalaBufferConverter

class ZarrOptions  extends Serializable {
  var bandNames = Array.fill(1)("Undefined")
  var numberBands = 1

  def setBands(nBands:Int, names:Option[util.ArrayList[String]]):Unit = {
    this.numberBands = nBands
    this.bandNames =
      if (names.isDefined){
        if (names.get.size()==nBands) {
          names.get.asScala.toArray[String]
        } else {
          Array.fill(nBands)("Undefined")
        }
      } else Array.fill(nBands)("Undefined")
  }

}
