package org.openeo.geotrellissentinelhub.bands

object Sentinel2L2ABands extends Bands {
  override val allBands: Seq[Sentinel2L2ABand] = Seq(B01, B02, B03, B04, B05, B06, B07, B08, B09, B11, B12, B8A)

  sealed trait Sentinel2L2ABand extends Band
  case object B01 extends Sentinel2L2ABand
  case object B02 extends Sentinel2L2ABand
  case object B03 extends Sentinel2L2ABand
  case object B04 extends Sentinel2L2ABand
  case object B05 extends Sentinel2L2ABand
  case object B06 extends Sentinel2L2ABand
  case object B07 extends Sentinel2L2ABand
  case object B08 extends Sentinel2L2ABand
  case object B09 extends Sentinel2L2ABand
  case object B11 extends Sentinel2L2ABand
  case object B12 extends Sentinel2L2ABand
  case object B8A extends Sentinel2L2ABand
}
