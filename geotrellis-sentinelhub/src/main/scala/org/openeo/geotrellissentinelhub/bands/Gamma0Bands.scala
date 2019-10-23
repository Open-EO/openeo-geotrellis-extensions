package org.openeo.geotrellissentinelhub.bands

object Gamma0Bands extends Bands {
  override val allBands: Seq[Gamma0Band] = Seq(IW_VV, IW_VH, IW_VV_DB, IW_VH_DB)

  sealed trait Gamma0Band extends Band
  case object IW_VV extends Gamma0Band
  case object IW_VH extends Gamma0Band
  case object IW_VV_DB extends Gamma0Band
  case object IW_VH_DB extends Gamma0Band
}
