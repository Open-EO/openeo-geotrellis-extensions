package org.openeo.geotrellisseeder

case class Band(name: String, min: Int, max: Int)

object Band {
  def apply(commaSeparatedString: String): Band = {
    val split = commaSeparatedString.split(",")
    
    Band(split(0), split(1).toInt, split(2).toInt)
  }
}
