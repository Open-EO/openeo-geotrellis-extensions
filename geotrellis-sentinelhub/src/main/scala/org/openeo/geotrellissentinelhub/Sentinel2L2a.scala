package org.openeo.geotrellissentinelhub

object Sentinel2L2a {
  private val tileIdPattern = raw"_T([0-9]{2}[A-Z]{3})_".r.unanchored

  def extractTileId(productId: String): Option[String] = productId match {
    case tileIdPattern(tileId) => Some(tileId)
    case _ => None
  }
}
