package org.openeo.geotrelliscommon

import scala.collection.JavaConverters._

object OpenEORasterCubeMetadata{

  def apply(): OpenEORasterCubeMetadata ={
    return new OpenEORasterCubeMetadata(Seq.empty)
  }
}

/**
 * Container object to attach metadata to OpenEO RasterCube, and provide a convenient interface towards Python.
 * @param bands
 */
class OpenEORasterCubeMetadata(var bands: Seq[String]) extends Serializable {
  var inputProducts:Seq[org.openeo.opensearch.OpenSearchResponses.Feature] = _


  def setBandNames(names:java.util.List[String]):Unit = {
    bands = names.asScala
  }

  def bandCount: Int = bands.size

}
