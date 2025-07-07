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

  /**
   * Avoids error when using .clone():
   * "method clone in class Object cannot be accessed in org.openeo.geotrelliscommon.OpenEORasterCubeMetadata"
   */
  def deepClone(): OpenEORasterCubeMetadata = {
    // https://www.avajava.com/tutorials/lessons/how-do-i-perform-a-deep-clone-using-serializable.html
    // TODO: Check for a better implementation
    val baos = new java.io.ByteArrayOutputStream()
    val oos = new java.io.ObjectOutputStream(baos)
    oos.writeObject(this)
    oos.close()
    val bais = new java.io.ByteArrayInputStream(baos.toByteArray())
    val ois = new java.io.ObjectInputStream(bais)
    ois.readObject().asInstanceOf[OpenEORasterCubeMetadata]
  }

}
