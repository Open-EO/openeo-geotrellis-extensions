package org.openeo.geotrelliscommon

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
  var inputProducts:Seq[be.vito.eodata.gwcgeotrellis.opensearch.OpenSearchResponses.Feature] = _


  def setBandNames(names:Array[String]):Unit = {
    bands = names.toSeq
  }

}