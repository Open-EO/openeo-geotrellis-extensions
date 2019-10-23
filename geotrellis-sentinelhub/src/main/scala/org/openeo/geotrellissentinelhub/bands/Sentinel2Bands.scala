package org.openeo.geotrellissentinelhub.bands

object Sentinel2Bands extends Bands {
  override val allBands: Seq[Sentinel2Band] = Seq(B01, B02, B03, B04, B05, B06, B07, B08, B09, B10, B11, B12, B8A)

  sealed trait Sentinel2Band extends Band
  case object B01 extends Sentinel2Band
  case object B02 extends Sentinel2Band
  case object B03 extends Sentinel2Band
  case object B04 extends Sentinel2Band
  case object B05 extends Sentinel2Band
  case object B06 extends Sentinel2Band
  case object B07 extends Sentinel2Band
  case object B08 extends Sentinel2Band
  case object B09 extends Sentinel2Band
  case object B10 extends Sentinel2Band
  case object B11 extends Sentinel2Band
  case object B12 extends Sentinel2Band
  case object B8A extends Sentinel2Band
}
