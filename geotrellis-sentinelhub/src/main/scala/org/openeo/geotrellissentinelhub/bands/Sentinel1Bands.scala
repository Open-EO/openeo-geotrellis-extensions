package org.openeo.geotrellissentinelhub.bands

object Sentinel1Bands extends Bands {
  override val allBands: Seq[Sentinel1Band] = Seq(VV, VH, HV, HH)

  sealed trait Sentinel1Band extends Band
  case object VV extends Sentinel1Band
  case object VH extends Sentinel1Band
  case object HV extends Sentinel1Band
  case object HH extends Sentinel1Band
}
