package org.openeo.geotrellissentinelhub

object Gamma0Bands {
  val gamma0Bands = Seq(IW_VV, IW_VH, IW_VV_DB, IW_VH_DB)

  sealed trait Band
  case object IW_VV extends Band
  case object IW_VH extends Band
  case object IW_VV_DB extends Band
  case object IW_VH_DB extends Band
}