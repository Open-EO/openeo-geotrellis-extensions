package org.openeo.geotrellissentinelhub.bands

object Sentinel1Bands extends Bands {
  override val allBands: Seq[Sentinel1Band] = Seq(IW_VV, IW_VH, IW_VV_DB, IW_VH_DB)

  sealed trait Sentinel1Band extends Band
  case object IW_VV extends Sentinel1Band
  case object IW_VH extends Sentinel1Band
  case object IW_VV_DB extends Sentinel1Band
  case object IW_VH_DB extends Sentinel1Band
}
