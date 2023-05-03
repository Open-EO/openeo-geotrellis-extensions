package org.openeo.geotrellisseeder

case class Band(name: String, id: Int, min: Int, max: Int, noData: Int)

object Band {
  def apply(name: String, min: Int, max: Int): Band = Band(name, 0, min, max, null)
  def apply(commaSeparatedString: String): Band = {
    val split = commaSeparatedString.split(",")

    if (split.length == 3) {
      Band(split(0), 0, split(1).toInt, split(2).toInt, null)
    } else if (split.length == 4) {
      Band(split(0), split(1).toInt, split(2).toInt, split(3).toInt, null)
    } else {
      Band(split(0), split(1).toInt, split(2).toInt, split(3).toInt, split(4).toInt)
    }
  }
}
