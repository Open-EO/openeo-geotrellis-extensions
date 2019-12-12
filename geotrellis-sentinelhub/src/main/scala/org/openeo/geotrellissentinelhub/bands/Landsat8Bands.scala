package org.openeo.geotrellissentinelhub.bands

object Landsat8Bands extends Bands {
  override val allBands: Seq[Landsat8Band] = Seq(B01, B02, B03, B04, B05, B06, B07, B08, B09, B10, B11)
  
  sealed trait Landsat8Band extends Band
  case object B01 extends Landsat8Band
  case object B02 extends Landsat8Band
  case object B03 extends Landsat8Band
  case object B04 extends Landsat8Band
  case object B05 extends Landsat8Band
  case object B06 extends Landsat8Band
  case object B07 extends Landsat8Band
  case object B08 extends Landsat8Band
  case object B09 extends Landsat8Band
  case object B10 extends Landsat8Band
  case object B11 extends Landsat8Band
}
